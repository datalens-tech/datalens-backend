#syntax=docker/dockerfile:1.4
FROM adoptopenjdk/openjdk11:alpine AS antlr_build

# Current Arcadian ANTLR version:
ENV ANTLR_VERSION 4.11.1
ENV CLASSPATH .:/antlr.jar:$CLASSPATH

RUN wget --no-check-certificate https://www.antlr.org/download/antlr-${ANTLR_VERSION}-complete.jar -O /antlr.jar
RUN chmod +r /antlr.jar

RUN apk add --no-cache sed python3 py3-pip
RUN pip install pycodestyle==2.10.0 autopep8==2.0.2 autoflake isort

ENV AUTOGEN_DIR=/gen
ENV ANTLR_PY_PACKAGE="bi_formula.parser.antlr.gen"

COPY --from=src . /src

RUN java -jar /antlr.jar -o $AUTOGEN_DIR -package $ANTLR_PY_PACKAGE -visitor -Dlanguage=Python3 -no-listener -lib "/src" "/src/DataLens.g4"
RUN touch $AUTOGEN_DIR/__init__.py
RUN sed --in-place='' \
    's/from antlr4 import [*]/from antlr4 import Lexer, LexerATNSimulator, ATNDeserializer, DFA, PredictionContextCache/' \
    "$AUTOGEN_DIR/DataLensLexer.py"
RUN sed --in-place='' \
    's/from antlr4 import [*]/from antlr4 import Parser, ATNDeserializer, DFA, PredictionContextCache, Token, TokenStream, ParserATNSimulator, RecognitionException, ParserRuleContext, ParseTreeVisitor, NoViableAltException, ATN, RuleContext/' \
    "$AUTOGEN_DIR/DataLensParser.py"
RUN sed --in-place='' 's/from antlr4 import [*]/from antlr4 import ParseTreeVisitor/' \
    "$AUTOGEN_DIR/DataLensVisitor.py"

RUN sed --in-place='' '/^# Generated from / d' "$AUTOGEN_DIR"/*.py
RUN sed --in-place='' 's/ *_prevctx = .*//' "$AUTOGEN_DIR/DataLensParser.py"
RUN sed --in-place='' 's/== None/is None/' "$AUTOGEN_DIR/DataLensParser.py"

RUN find "$AUTOGEN_DIR" -type f -name '*.py' -print0 | xargs -0 autopep8 -i --max-line-length 200
RUN find "$AUTOGEN_DIR" -type f -name '*.py' -print0 | xargs -0 autoflake --in-place --remove-unused-variables
RUN find "$AUTOGEN_DIR" -type f -name '*.py' -print0 | xargs -0 isort --line-length 200

FROM scratch

COPY --from=antlr_build /gen .
