from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.utils import get_and_update_current_user, get_session
from datajunction_server.models.role import (
    AccessRuleCreate,
    AccessRuleOutput,
    RoleCreate,
    RoleUpdate,
    RoleOut,
)
from datajunction_server.database.role import Role, AccessRule
from datajunction_server.database.user import User

router = SecureAPIRouter(prefix="/roles", tags=["roles"])


@router.get("/", response_model=list[RoleOut])
async def list_roles(session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(Role))
    roles = result.scalars().all()
    return roles


@router.post("/", response_model=RoleOut, status_code=201)
async def create_role(
    role_in: RoleCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
):
    role = Role(**role_in.dict())
    role.created_by_id = current_user.id
    session.add(role)
    await session.commit()
    await session.refresh(role)
    return role


@router.get("/{role_id}", response_model=RoleOut)
async def get_role(role_id: int, session: AsyncSession = Depends(get_session)):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    return role


@router.get("/{role_id}/access-rules")
async def list_role_rules(
    role_id: int,
    session: AsyncSession = Depends(get_session),
) -> list[AccessRuleOutput]:
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    print("role.access_rules", role.access_rules)
    return role.access_rules  # assumes relationship exists


@router.post("/{role_id}/access-rules", status_code=201)
async def upsert_rule_for_role(
    role_id: int,
    rule_create: AccessRuleCreate,
    session: AsyncSession = Depends(get_session),
):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    if not any(rule_create.equivalent(rule) for rule in role.access_rules):
        new_rule = AccessRule(**rule_create.dict(), role_id=role.id)
        role.access_rules.append(new_rule)
        session.add(role)
        await session.commit()

    return {"message": "Rule added"}


@router.patch("/{role_id}", response_model=RoleOut)
async def update_role(
    role_id: int,
    role_in: RoleUpdate,
    session: AsyncSession = Depends(get_session),
):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    for key, value in role_in.dict(exclude_unset=True).items():
        setattr(role, key, value)
    session.add(role)
    await session.commit()
    await session.refresh(role)
    return role


@router.delete("/{role_id}", status_code=204)
async def delete_role(role_id: int, session: AsyncSession = Depends(get_session)):
    role = await session.get(Role, role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    await session.delete(role)
    await session.commit()
