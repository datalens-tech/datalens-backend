import attr
from typing import Collection

import dl_formula.core.nodes as nodes
import dl_formula.core.exc as exc
import dl_formula.inspect.expression
import dl_formula.inspect.node
from dl_formula.validation.validator import (
    Checker,
    ValidatorProxy,
)


@attr.s
class BFBChecker(Checker):
    _field_ids: Collection[str] = attr.ib(factory=frozenset)

    def perform_node_check(
        self,
        validator: ValidatorProxy,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> None:
        children_w_stacks = dl_formula.inspect.expression.autonomous_children_w_stack(
            node,
            parent_stack=parent_stack,
        )
        for child, stack in children_w_stacks:
            with validator.handle_error(node=node):
                self.check_node(validator=validator, node=child, parent_stack=stack)
    
        if not isinstance(node, nodes.FuncCall):
            return
        
        bfb = node.before_filter_by
        for bfb_name in bfb.field_names:
            if bfb_name not in self._field_ids:
                with validator.handle_error(node=node):
                    raise exc.UnknownBFBFieldError(
                        f"Function's BEFORE FILTER BY clause refers to unknown field {bfb_name}",
                        token=dl_formula.inspect.node.get_token(bfb),
                        position=bfb.position,
                    )
