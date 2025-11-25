from fastapi import APIRouter, HTTPException
from typing import List

from app.api.schemas.shared import (
    LlmInstructionProfile,
    LlmInstructionListResponse,
    CreateLlmInstructionRequest,
    UpdateLlmInstructionRequest,
)
from app.db.llm_instructions import (
    list_llm_instructions,
    insert_llm_instruction,
    get_llm_instruction,
    touch_llm_instruction,
    update_llm_instruction,
    delete_llm_instruction,
    create_llm_instruction_table,
)

router = APIRouter(prefix="/llm-instructions", tags=["llm-instructions"])


@router.get("", response_model=LlmInstructionListResponse)
def list_instructions(limit: int = 50):
    """Return recently saved LLM instruction profiles."""
    create_llm_instruction_table()
    limit = max(1, min(limit, 200))
    instructions = list_llm_instructions(limit=limit)
    return LlmInstructionListResponse(success=True, instructions=instructions)


@router.post("", response_model=LlmInstructionProfile)
def create_instruction(request: CreateLlmInstructionRequest):
    """Save a reusable LLM instruction profile."""
    create_llm_instruction_table()
    normalized_title = (request.title or "").strip()
    normalized_content = (request.content or "").strip()
    if not normalized_title:
        raise HTTPException(status_code=400, detail="Title is required")
    if not normalized_content:
        raise HTTPException(status_code=400, detail="Content is required")

    instruction_id = insert_llm_instruction(normalized_title, normalized_content)
    saved = get_llm_instruction(instruction_id)
    if not saved:
        raise HTTPException(status_code=500, detail="Instruction could not be saved")
    return LlmInstructionProfile(**saved)


@router.get("/{instruction_id}", response_model=LlmInstructionProfile)
def get_instruction(instruction_id: str):
    """Fetch a specific instruction profile."""
    create_llm_instruction_table()
    record = get_llm_instruction(instruction_id)
    if not record:
        raise HTTPException(status_code=404, detail="Instruction not found")
    touch_llm_instruction(instruction_id)
    return LlmInstructionProfile(**record)


@router.put("/{instruction_id}", response_model=LlmInstructionProfile)
@router.patch("/{instruction_id}", response_model=LlmInstructionProfile)
def update_instruction(instruction_id: str, request: UpdateLlmInstructionRequest):
    """Update an instruction profile (title, content)."""
    create_llm_instruction_table()
    if request.title is None and request.content is None:
        raise HTTPException(status_code=400, detail="Provide a title or content to update")

    normalized_title = (
        request.title.strip() if isinstance(request.title, str) else request.title
    )
    normalized_content = (
        request.content.strip() if isinstance(request.content, str) else request.content
    )

    updated = update_llm_instruction(
        instruction_id, title=normalized_title, content=normalized_content
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Instruction not found")
    return LlmInstructionProfile(**updated)


@router.delete("/{instruction_id}")
def delete_instruction(instruction_id: str):
    """Delete an instruction profile."""
    create_llm_instruction_table()
    deleted = delete_llm_instruction(instruction_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Instruction not found")
    return {"success": True}
