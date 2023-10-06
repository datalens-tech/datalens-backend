from antlr4 import ParseTreeVisitor

if __name__ is not None and "." in __name__:
    from .DataLensParser import DataLensParser
else:
    from DataLensParser import DataLensParser

# This class defines a complete generic visitor for a parse tree produced by DataLensParser.


class DataLensVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by DataLensParser#integerLiteral.
    def visitIntegerLiteral(self, ctx: DataLensParser.IntegerLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#floatLiteral.
    def visitFloatLiteral(self, ctx: DataLensParser.FloatLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#stringLiteral.
    def visitStringLiteral(self, ctx: DataLensParser.StringLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#dateLiteral.
    def visitDateLiteral(self, ctx: DataLensParser.DateLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#datetimeLiteral.
    def visitDatetimeLiteral(self, ctx: DataLensParser.DatetimeLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#genericDateLiteral.
    def visitGenericDateLiteral(self, ctx: DataLensParser.GenericDateLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#genericDatetimeLiteral.
    def visitGenericDatetimeLiteral(self, ctx: DataLensParser.GenericDatetimeLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#boolLiteral.
    def visitBoolLiteral(self, ctx: DataLensParser.BoolLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#nullLiteral.
    def visitNullLiteral(self, ctx: DataLensParser.NullLiteralContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#fieldName.
    def visitFieldName(self, ctx: DataLensParser.FieldNameContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#orderingItem.
    def visitOrderingItem(self, ctx: DataLensParser.OrderingItemContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#ordering.
    def visitOrdering(self, ctx: DataLensParser.OrderingContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#lodSpecifier.
    def visitLodSpecifier(self, ctx: DataLensParser.LodSpecifierContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#winGrouping.
    def visitWinGrouping(self, ctx: DataLensParser.WinGroupingContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#beforeFilterBy.
    def visitBeforeFilterBy(self, ctx: DataLensParser.BeforeFilterByContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#ignoreDimensions.
    def visitIgnoreDimensions(self, ctx: DataLensParser.IgnoreDimensionsContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#function.
    def visitFunction(self, ctx: DataLensParser.FunctionContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#elseifPart.
    def visitElseifPart(self, ctx: DataLensParser.ElseifPartContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#elsePart.
    def visitElsePart(self, ctx: DataLensParser.ElsePartContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#ifPart.
    def visitIfPart(self, ctx: DataLensParser.IfPartContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#ifBlock.
    def visitIfBlock(self, ctx: DataLensParser.IfBlockContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#whenPart.
    def visitWhenPart(self, ctx: DataLensParser.WhenPartContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#caseBlock.
    def visitCaseBlock(self, ctx: DataLensParser.CaseBlockContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#parenthesizedExpr.
    def visitParenthesizedExpr(self, ctx: DataLensParser.ParenthesizedExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#exprBasic.
    def visitExprBasic(self, ctx: DataLensParser.ExprBasicContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#exprBasicAlt.
    def visitExprBasicAlt(self, ctx: DataLensParser.ExprBasicAltContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#unaryPrefix.
    def visitUnaryPrefix(self, ctx: DataLensParser.UnaryPrefixContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#inExpr.
    def visitInExpr(self, ctx: DataLensParser.InExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#binaryExpr.
    def visitBinaryExpr(self, ctx: DataLensParser.BinaryExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#comparisonChain.
    def visitComparisonChain(self, ctx: DataLensParser.ComparisonChainContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#unaryPostfix.
    def visitUnaryPostfix(self, ctx: DataLensParser.UnaryPostfixContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#betweenExpr.
    def visitBetweenExpr(self, ctx: DataLensParser.BetweenExprContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#binaryExprSec.
    def visitBinaryExprSec(self, ctx: DataLensParser.BinaryExprSecContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#exprMainAlt.
    def visitExprMainAlt(self, ctx: DataLensParser.ExprMainAltContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#expression.
    def visitExpression(self, ctx: DataLensParser.ExpressionContext):
        return self.visitChildren(ctx)

    # Visit a parse tree produced by DataLensParser#parse.
    def visitParse(self, ctx: DataLensParser.ParseContext):
        return self.visitChildren(ctx)


del DataLensParser
