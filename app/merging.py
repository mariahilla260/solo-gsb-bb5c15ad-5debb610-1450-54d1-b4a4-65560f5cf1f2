"""重复试剂档案合并：先预览、再凭一次性确认令牌在单事务内迁移。

流程（需求约束）：

1. **预览**（``preview_merge``）：选择来源档案与保留档案，返回受影响的
   方案 / 步骤数、两侧名称与类别，并生成一个**仅对本次预览有效**的确认令牌
   （内存保存、10 分钟过期、一次性）；
2. **确认**（``confirm_merge``）：重新核对两侧试剂属性签名与全部受影响步骤的
   内容签名——预览后任一档案属性或受影响步骤发生变化（含步骤被删除 / 新建了
   引用来源档案的步骤）即拒绝提交（409 stale_preview），要求重新预览；
3. 校验通过后在**单个事务**内：把来源档案的步骤引用迁到保留档案、
   更新步骤展示快照（名称）、把这些步骤的安全确认统一置为**待复核**，
   最后删除来源档案；任一步失败整体回滚，不留下部分迁移。

历史推演报告（runs.report）是自包含快照，合并过程不触碰，保持原样。
"""
import hashlib
import json
import secrets
import threading
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from sqlalchemy.orm import Session

from . import models

# 预览令牌有效期：仅对当次预览有效，过期需重新预览
TOKEN_TTL = timedelta(minutes=10)

# 进程内令牌库（单实例部署 + 同一事务语义；重启后旧令牌自然失效）
_tokens: dict[str, dict] = {}
_tokens_lock = threading.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hash(payload) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def reagent_categories(reagent: models.Reagent) -> list[str]:
    return sorted(json.loads(reagent.categories or "[]"))


def reagent_snapshot(reagent: models.Reagent) -> dict:
    """试剂档案对外展示快照（预览 / 确认响应共用）。"""
    return {
        "id": reagent.id,
        "name": reagent.name,
        "cas": reagent.cas,
        "formula": reagent.formula,
        "concentration": reagent.concentration,
        "state": reagent.state,
        "categories": reagent_categories(reagent),
        "builtin": bool(reagent.builtin),
    }


def reagent_signature(reagent: models.Reagent) -> str:
    """试剂属性签名：名称 / CAS / 分子式 / 浓度 / 物态 / 类别任一变化即失配。"""
    return _hash(
        [
            reagent.name,
            reagent.cas,
            reagent.formula,
            reagent.concentration,
            reagent.state,
            reagent_categories(reagent),
        ]
    )


def step_signature(step: models.Step) -> str:
    """受影响步骤内容签名：预览后步骤被修改 / 删除 / 新建均可识别。

    除步骤业务内容外，安全确认相关字段也纳入签名——预览后若确认状态发生
    变化，同样要求重新预览，避免在操作者不知情的情况下把刚完成的确认
    覆盖成"待复核"。
    """
    return _hash(
        [
            step.id,
            step.plan_id,
            step.order_index,
            None if step.time_offset_min is None else float(step.time_offset_min),
            step.reagent_id,
            step.reagent_name,
            json.loads(step.categories or "[]"),
            None if step.amount_ml is None else float(step.amount_ml),
            step.note,
            json.loads(step.safety_measures or "[]"),
            step.safety_note,
            bool(step.safety_confirmed),
            step.safety_confirmed_at.isoformat() if step.safety_confirmed_at else None,
            step.safety_confirmed_fp,
            bool(step.safety_stale),
        ]
    )


def _affected_steps(db: Session, source_id: int) -> list[models.Step]:
    return (
        db.query(models.Step)
        .filter(models.Step.reagent_id == source_id)
        .order_by(models.Step.plan_id, models.Step.order_index, models.Step.id)
        .all()
    )


def _affected_preview(db: Session, steps: list[models.Step]) -> tuple[list[dict], dict[int, str]]:
    """构造受影响方案 / 步骤明细与 {step_id: 步骤签名}。"""
    plan_ids = sorted({s.plan_id for s in steps})
    plans = {p.id: p for p in db.query(models.Plan).filter(models.Plan.id.in_(plan_ids)).all()}
    grouped: dict[int, list[models.Step]] = {}
    for s in steps:
        grouped.setdefault(s.plan_id, []).append(s)
    plans_out = []
    signatures: dict[int, str] = {}
    for plan_id in sorted(grouped):
        plan_steps = grouped[plan_id]
        plan = plans.get(plan_id)
        plans_out.append(
            {
                "plan_id": plan_id,
                "plan_name": plan.name if plan is not None else f"方案 #{plan_id}（已删除）",
                "step_count": len(plan_steps),
                "steps": [
                    {
                        "step_id": s.id,
                        "order_index": s.order_index,
                        "reagent_name": s.reagent_name,
                    }
                    for s in sorted(plan_steps, key=lambda x: (x.order_index, x.id))
                ],
            }
        )
        for s in plan_steps:
            signatures[s.id] = step_signature(s)
    return plans_out, signatures


def _purge_expired(now: datetime) -> None:
    expired = [token for token, item in _tokens.items() if item["expires_at"] <= now]
    for token in expired:
        _tokens.pop(token, None)


def preview_merge(db: Session, source_id: int, target_id: int) -> dict:
    """生成合并预览与一次性确认令牌（不落库）。"""
    if source_id == target_id:
        raise HTTPException(422, "来源档案与保留档案不能是同一个试剂")
    source = db.get(models.Reagent, source_id)
    if source is None:
        raise HTTPException(404, f"来源试剂 id={source_id} 不存在")
    target = db.get(models.Reagent, target_id)
    if target is None:
        raise HTTPException(404, f"保留试剂 id={target_id} 不存在")

    steps = _affected_steps(db, source_id)
    affected_plans, step_sigs = _affected_preview(db, steps)

    now = _utcnow()
    token = secrets.token_urlsafe(24)
    with _tokens_lock:
        _purge_expired(now)
        _tokens[token] = {
            "source_id": source_id,
            "target_id": target_id,
            "source_sig": reagent_signature(source),
            "target_sig": reagent_signature(target),
            "step_sigs": step_sigs,
            "created_at": now,
            "expires_at": now + TOKEN_TTL,
        }

    return {
        "kind": "labmix-reagent-merge-preview",
        "source": reagent_snapshot(source),
        "target": reagent_snapshot(target),
        "affected_step_count": len(steps),
        "affected_plan_count": len(affected_plans),
        "affected_plans": affected_plans,
        "token": token,
        "expires_at": (now + TOKEN_TTL).isoformat(),
        "message": (
            f"合并后 {len(steps)} 个加液步骤将改引用保留档案「{target.name}」，"
            "这些步骤的安全确认将转为待复核，来源档案将被删除。"
            "历史推演报告保持原快照不变。"
        ),
    }


def _invalid_token() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": "invalid_merge_token",
            "message": "确认令牌无效或已过期（每个令牌只能使用一次），请重新预览后再确认合并。",
        },
    )


def _stale_preview(changes: list[str]) -> HTTPException:
    labels = {
        "source": "来源档案属性已变化",
        "target": "保留档案属性已变化",
        "steps": "受影响的方案步骤已变化",
        "source_missing": "来源档案已不存在",
        "target_missing": "保留档案已不存在",
    }
    return HTTPException(
        status_code=409,
        detail={
            "code": "stale_preview",
            "message": "预览后数据已变化（"
            + "、".join(labels.get(c, c) for c in changes)
            + "），合并已拒绝，请重新预览后再确认。",
            "changes": changes,
        },
    )


def confirm_merge(db: Session, source_id: int, target_id: int, token: str) -> dict:
    """凭预览令牌执行合并；状态变化或令牌失效一律拒绝，不产生部分迁移。"""
    if not token:
        raise _invalid_token()

    # 令牌一次性：无论后续校验是否通过，取出即作废
    with _tokens_lock:
        _purge_expired(_utcnow())
        saved = _tokens.pop(token, None)
    if saved is None:
        raise _invalid_token()
    if saved["source_id"] != source_id or saved["target_id"] != target_id:
        raise _stale_preview(["source", "target"])
    if saved["expires_at"] <= _utcnow():
        raise _invalid_token()

    source = db.get(models.Reagent, source_id)
    target = db.get(models.Reagent, target_id)
    changes: list[str] = []
    if source is None:
        changes.append("source_missing")
    if target is None:
        changes.append("target_missing")
    if source is not None and reagent_signature(source) != saved["source_sig"]:
        changes.append("source")
    if target is not None and reagent_signature(target) != saved["target_sig"]:
        changes.append("target")
    if changes:
        raise _stale_preview(changes)

    steps = _affected_steps(db, source_id)
    current_sigs = {s.id: step_signature(s) for s in steps}
    if current_sigs != saved["step_sigs"]:
        # 步骤被修改 / 删除，或预览后又有新步骤引用了来源档案
        raise _stale_preview(["steps"])

    affected_plans, _ = _affected_preview(db, steps)
    plan_ids = sorted({s.plan_id for s in steps})

    try:
        # ① 迁移步骤引用 + 更新展示快照；② 安全确认统一转待复核；
        # ③ 删除来源档案——全部在同一事务内完成
        touched_plans = (
            db.query(models.Plan).filter(models.Plan.id.in_(plan_ids)).all() if plan_ids else []
        )
        for step in steps:
            step.reagent_id = target.id
            step.reagent_name = target.name
            # 档案试剂类别始终从保留档案实时解析，步骤快照留空（与手工建步一致）
            step.categories = "[]"
            step.safety_stale = True
        for plan in touched_plans:
            plan.updated_at = _utcnow()
        db.flush()
        # 使来源档案的关系集合过期，避免 ORM 把刚迁移的步骤按关系同步置空
        db.expire(source, ["steps"])
        db.delete(source)
        db.commit()
    except Exception:
        db.rollback()
        raise

    db.refresh(target)
    return {
        "kind": "labmix-reagent-merge",
        "merged": True,
        "source": {"id": source_id},
        "target": reagent_snapshot(target),
        "migrated_step_count": len(steps),
        "affected_plan_count": len(affected_plans),
        "affected_plans": affected_plans,
        "message": (
            f"已合并到保留档案「{target.name}」：迁移 {len(steps)} 个步骤，"
            "这些步骤的安全确认已转为待复核，来源档案已删除；历史推演报告保持原快照。"
        ),
    }
