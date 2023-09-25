grammar DataLens;

/*
 * Lexer Rules
 */

// Comments and whitespaces -> skip

SINGLE_LINE_COMMENT: '--' ~[\n]* ('\n'|EOF) -> skip ;
MULTI_LINE_COMMENT : '/*' ( ~[*] | [*]+ ~[*/] )* [*]* '*/' -> skip ;
WS                 : [ \t\n\r\f\u00a0]+ -> skip ;


// Letters for keywords
fragment A          : ('A'|'a') ;
fragment B          : ('B'|'b') ;
fragment C          : ('C'|'c') ;
fragment D          : ('D'|'d') ;
fragment E          : ('E'|'e') ;
fragment F          : ('F'|'f') ;
fragment G          : ('G'|'g') ;
fragment H          : ('H'|'h') ;
fragment I          : ('I'|'i') ;
fragment K          : ('K'|'k') ;
fragment L          : ('L'|'l') ;
fragment M          : ('M'|'m') ;
fragment N          : ('N'|'n') ;
fragment O          : ('O'|'o') ;
fragment R          : ('R'|'r') ;
fragment S          : ('S'|'s') ;
fragment T          : ('T'|'t') ;
fragment U          : ('U'|'u') ;
fragment W          : ('W'|'w') ;
fragment X          : ('X'|'x') ;
fragment Y          : ('Y'|'y') ;


// Keyword definitions

AMONG              : A M O N G ;
AND                : A N D ;
ASC                : A S C ;
BEFORE_FILTER_BY   : B E F O R E ' '+ F I L T E R ' '+ B Y ;
BETWEEN            : B E T W E E N ;
CASE               : C A S E ;
DESC               : D E S C ;
ELSE               : E L S E ;
ELSEIF             : E L S E I F ;
END                : E N D ;
EXCLUDE            : E X C L U D E ;
FALSE              : F A L S E ;
FIXED              : F I X E D ;
IF                 : I F ;
IGNORE_DIMENSIONS  : I G N O R E ' '+ D I M E N S I O N S ;
IN                 : I N ;
INCLUDE            : I N C L U D E ;
IS                 : I S ;
LIKE               : L I K E ;
NOT                : N O T ;
NULL               : N U L L ;
OR                 : O R ;
ORDER_BY           : O R D E R ' '+ B Y ;
THEN               : T H E N ;
TOTAL              : T O T A L ;
TRUE               : T R U E ;
WHEN               : W H E N ;
WITHIN             : W I T H I N ;


// Operators

PLUS       : '+' ;
MINUS      : '-' ;
POWER      : '^' ;
MULDIV     : '*'|'/'|'%' ;
COMPARISON : '='|'!='|'<>'|'>'|'>='|'<'|'<=' ;


// Parentheses

OPENING_PAR   : '(' ;
CLOSING_PAR   : ')' ;


// Numbers

fragment DIGIT       : [0-9] ;
fragment LETTER      : [a-zA-Z] ;
fragment DECIMAL     : INT '.' INT? | '.' INT ;
fragment EXP         : [eE] [+\-]? INT ;
INT         : DIGIT+ ;
FLOAT       : INT EXP | DECIMAL EXP? ;


// Strings

ESCAPED_STRING
    : '\'' ( '\\\'' | ~['] )* '\''
    | '"' ( '\\"' | ~["] )* '"'
    ;


// A field name must not contain square brackets [] or curly braces {}
FIELD_NAME  : '[' ~[[\]{}]+ ']' ;

// A function name can only contain latin letters, numbers and underscores
FUNC_NAME   : ( LETTER | '_' ) ( LETTER | DIGIT | '_' )* ;


// Date & datetime literals

fragment DD: [0-9][0-9] ;
fragment DDDD: [0-9][0-9][0-9][0-9] ;
DATE_INNER
    : DDDD '-' DD '-' DD
    ;
DATETIME_INNER
    : DDDD '-' DD '-' DD [Tt ] DD ':' DD ':' DD
    | DDDD '-' DD '-' DD [Tt ] DD ':' DD
    | DDDD '-' DD '-' DD [Tt ] DD
    | DDDD '-' DD '-' DD [Tt]
    ;


// The following rule will allow the lexer to always finish without errors,
// so that all error handling can be done by the parser.
UNEXPECTED_CHARACTER: . ;


/*
 * Parser Rules
 */

integerLiteral   : INT ;
floatLiteral     : FLOAT ;
stringLiteral    : ESCAPED_STRING ;
dateLiteral      : '#' DATE_INNER '#' ;
datetimeLiteral  : '#' DATETIME_INNER '#' ;
genericDateLiteral      : '##' DATE_INNER '##' ;
genericDatetimeLiteral  : '##' DATETIME_INNER '##' ;
boolLiteral      : TRUE
                 | FALSE ;
nullLiteral      : NULL ;

fieldName: FIELD_NAME ;

orderingItem
    : expression
    | expression ASC
    | expression DESC
    ;
ordering: ORDER_BY orderingItem (',' orderingItem)* ;

lodSpecifier
    : FIXED (expression (',' expression)*)?
    | INCLUDE (expression (',' expression)*)?
    | EXCLUDE (expression (',' expression)*)?
    ;
winGrouping
    : TOTAL
    | AMONG (expression (',' expression)*)?
    | WITHIN (expression (',' expression)*)?
    ;
beforeFilterBy: BEFORE_FILTER_BY (fieldName (',' fieldName)*)? ;
ignoreDimensions: IGNORE_DIMENSIONS (fieldName (',' fieldName)*)? ;
function
    : (FUNC_NAME|CASE|IF|NOT|AND|OR|LIKE|BETWEEN) OPENING_PAR (expression (',' expression)*)? winGrouping? ordering? lodSpecifier? beforeFilterBy? ignoreDimensions? CLOSING_PAR
    ;

elseifPart : ELSEIF expression THEN expression ;
elsePart   : ELSE expression ;
ifPart     : IF expression THEN expression ;
ifBlock    : ifPart elseifPart* elsePart? END ;
whenPart   : WHEN expression THEN expression ;
caseBlock  : CASE expression whenPart+ elsePart? END ;

parenthesizedExpr: OPENING_PAR expression CLOSING_PAR ;

exprBasic
    : integerLiteral
    | floatLiteral
    | boolLiteral
    | nullLiteral
    | ifBlock
    | caseBlock
    | stringLiteral
    | fieldName
    | dateLiteral
    | datetimeLiteral
    | genericDateLiteral
    | genericDatetimeLiteral
    | function
    | parenthesizedExpr
    ;

exprMain
    : exprMain POWER exprMain                                                   # binaryExpr
    | MINUS exprMain                                                            # unaryPrefix
    | exprMain MULDIV exprMain                                                  # binaryExpr
    | exprMain (PLUS|MINUS) exprMain                                            # binaryExpr
    | exprMain IS NOT? (TRUE|FALSE|NULL)                                        # unaryPostfix
    | exprMain NOT? LIKE exprMain                                               # binaryExpr
    | exprMain COMPARISON exprMain                                              # comparisonChain
    | exprMain ( BETWEEN | NOT BETWEEN ) exprMain AND exprMain                  # betweenExpr
    | exprMain NOT? IN OPENING_PAR ((expression ',')* expression)? CLOSING_PAR  # inExpr
    | NOT exprMain                                                              # unaryPrefix
    | exprBasic                                                                 # exprBasicAlt
    ;

 exprSecondary
    : exprSecondary AND exprSecondary                                           # binaryExprSec
    | exprSecondary OR exprSecondary                                            # binaryExprSec
    | exprMain                                                                  # exprMainAlt
    ;

expression: exprSecondary;

parse
    : expression EOF
    | EOF
    ;
