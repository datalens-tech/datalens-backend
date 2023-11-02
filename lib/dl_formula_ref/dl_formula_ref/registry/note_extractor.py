from __future__ import annotations

from typing import TYPE_CHECKING

from dl_formula_ref.registry.arg_common import TypeStrategyInspector
from dl_formula_ref.registry.note import (
    NoteType,
    ParameterizedNote,
)
from dl_formula_ref.registry.note_extr_base import NoteExtractorBase
from dl_formula_ref.registry.text import ParameterizedText
from dl_formula_ref.texts import CONST_TYPE_NOTE


if TYPE_CHECKING:
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


class DefaultNoteExtractor(NoteExtractorBase):
    def get_notes(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> list[ParameterizedNote]:
        # explicitly defined notes
        notes = [
            ParameterizedNote(
                param_text=ParameterizedText.from_str(text=note.text),
                level=note.level,
                formatting=note.formatting,
                type=NoteType.REGULAR,
            )
            for note in item.get_explicit_notes(env=env)
        ]
        args = item.get_args(env=env)

        ts_insp = TypeStrategyInspector()

        # note about args that have to be of the same type (deduced from the function's return type strategy)
        type_info = ts_insp.get_return_type_and_arg_type_note(item, env=env)
        arg_type_note = type_info.arg_note
        if arg_type_note is not None:
            notes.append(
                ParameterizedNote(
                    param_text=arg_type_note,
                    type=NoteType.ARG_RESTRICTION,
                )
            )

        # note about args that must be const
        if any(a.is_const for a in args):
            notes.insert(
                0,
                ParameterizedNote(
                    param_text=ParameterizedText.from_str(
                        text=CONST_TYPE_NOTE,
                        params=dict(args=", ".join([f"`{a.name}`" for a in args if a.is_const])),
                    ),
                    type=NoteType.ARG_RESTRICTION,
                ),
            )
        return notes
