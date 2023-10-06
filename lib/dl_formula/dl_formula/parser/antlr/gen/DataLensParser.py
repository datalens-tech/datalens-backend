# encoding: utf-8
import sys

from antlr4 import (ATN, DFA, ATNDeserializer, NoViableAltException, Parser, ParserATNSimulator, ParserRuleContext, ParseTreeVisitor, PredictionContextCache, RecognitionException, RuleContext, Token,
                    TokenStream)

if sys.version_info[1] > 5:
    from typing import TextIO
else:
    from typing.io import TextIO


def serializedATN():
    return [
        4, 1, 49, 365, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7, 4, 2, 5, 7, 5, 2, 6, 7,
        6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13,
        2, 14, 7, 14, 2, 15, 7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20,
        7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 7, 24, 2, 25, 7, 25, 2, 26, 7, 26,
        2, 27, 7, 27, 2, 28, 7, 28, 1, 0, 1, 0, 1, 1, 1, 1, 1, 2, 1, 2, 1, 3, 1, 3, 1, 3, 1, 3, 1, 4,
        1, 4, 1, 4, 1, 4, 1, 5, 1, 5, 1, 5, 1, 5, 1, 6, 1, 6, 1, 6, 1, 6, 1, 7, 1, 7, 1, 8, 1, 8, 1, 9,
        1, 9, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 3, 10, 94, 8, 10, 1, 11, 1, 11, 1,
        11, 1, 11, 5, 11, 100, 8, 11, 10, 11, 12, 11, 103, 9, 11, 1, 12, 1, 12, 1, 12, 1, 12, 5,
        12, 109, 8, 12, 10, 12, 12, 12, 112, 9, 12, 3, 12, 114, 8, 12, 1, 12, 1, 12, 1, 12, 1,
        12, 5, 12, 120, 8, 12, 10, 12, 12, 12, 123, 9, 12, 3, 12, 125, 8, 12, 1, 12, 1, 12, 1,
        12, 1, 12, 5, 12, 131, 8, 12, 10, 12, 12, 12, 134, 9, 12, 3, 12, 136, 8, 12, 3, 12, 138,
        8, 12, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 5, 13, 145, 8, 13, 10, 13, 12, 13, 148, 9, 13,
        3, 13, 150, 8, 13, 1, 13, 1, 13, 1, 13, 1, 13, 5, 13, 156, 8, 13, 10, 13, 12, 13, 159,
        9, 13, 3, 13, 161, 8, 13, 3, 13, 163, 8, 13, 1, 14, 1, 14, 1, 14, 1, 14, 5, 14, 169, 8,
        14, 10, 14, 12, 14, 172, 9, 14, 3, 14, 174, 8, 14, 1, 15, 1, 15, 1, 15, 1, 15, 5, 15, 180,
        8, 15, 10, 15, 12, 15, 183, 9, 15, 3, 15, 185, 8, 15, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16,
        5, 16, 192, 8, 16, 10, 16, 12, 16, 195, 9, 16, 3, 16, 197, 8, 16, 1, 16, 3, 16, 200, 8,
        16, 1, 16, 3, 16, 203, 8, 16, 1, 16, 3, 16, 206, 8, 16, 1, 16, 3, 16, 209, 8, 16, 1, 16,
        3, 16, 212, 8, 16, 1, 16, 1, 16, 1, 17, 1, 17, 1, 17, 1, 17, 1, 17, 1, 18, 1, 18, 1, 18,
        1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 20, 1, 20, 5, 20, 231, 8, 20, 10, 20, 12, 20, 234,
        9, 20, 1, 20, 3, 20, 237, 8, 20, 1, 20, 1, 20, 1, 21, 1, 21, 1, 21, 1, 21, 1, 21, 1, 22,
        1, 22, 1, 22, 4, 22, 249, 8, 22, 11, 22, 12, 22, 250, 1, 22, 3, 22, 254, 8, 22, 1, 22,
        1, 22, 1, 23, 1, 23, 1, 23, 1, 23, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24,
        1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 3, 24, 276, 8, 24, 1, 25, 1, 25, 1, 25, 1, 25,
        1, 25, 1, 25, 3, 25, 284, 8, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25,
        1, 25, 1, 25, 1, 25, 3, 25, 297, 8, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25,
        1, 25, 1, 25, 3, 25, 308, 8, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 3, 25,
        317, 8, 25, 1, 25, 1, 25, 1, 25, 3, 25, 322, 8, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 5,
        25, 329, 8, 25, 10, 25, 12, 25, 332, 9, 25, 1, 25, 3, 25, 335, 8, 25, 1, 25, 5, 25, 338,
        8, 25, 10, 25, 12, 25, 341, 9, 25, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26,
        1, 26, 5, 26, 352, 8, 26, 10, 26, 12, 26, 355, 9, 26, 1, 27, 1, 27, 1, 28, 1, 28, 1, 28,
        1, 28, 3, 28, 363, 8, 28, 1, 28, 0, 2, 50, 52, 29, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20,
        22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 0, 4, 2, 0, 18,
        18, 32, 32, 6, 0, 8, 8, 11, 12, 20, 20, 25, 26, 28, 28, 46, 46, 1, 0, 35, 36, 3, 0, 18,
        18, 27, 27, 32, 32, 399, 0, 58, 1, 0, 0, 0, 2, 60, 1, 0, 0, 0, 4, 62, 1, 0, 0, 0, 6, 64, 1,
        0, 0, 0, 8, 68, 1, 0, 0, 0, 10, 72, 1, 0, 0, 0, 12, 76, 1, 0, 0, 0, 14, 80, 1, 0, 0, 0, 16,
        82, 1, 0, 0, 0, 18, 84, 1, 0, 0, 0, 20, 93, 1, 0, 0, 0, 22, 95, 1, 0, 0, 0, 24, 137, 1, 0,
        0, 0, 26, 162, 1, 0, 0, 0, 28, 164, 1, 0, 0, 0, 30, 175, 1, 0, 0, 0, 32, 186, 1, 0, 0, 0,
        34, 215, 1, 0, 0, 0, 36, 220, 1, 0, 0, 0, 38, 223, 1, 0, 0, 0, 40, 228, 1, 0, 0, 0, 42, 240,
        1, 0, 0, 0, 44, 245, 1, 0, 0, 0, 46, 257, 1, 0, 0, 0, 48, 275, 1, 0, 0, 0, 50, 283, 1, 0,
        0, 0, 52, 342, 1, 0, 0, 0, 54, 356, 1, 0, 0, 0, 56, 362, 1, 0, 0, 0, 58, 59, 5, 42, 0, 0,
        59, 1, 1, 0, 0, 0, 60, 61, 5, 43, 0, 0, 61, 3, 1, 0, 0, 0, 62, 63, 5, 44, 0, 0, 63, 5, 1, 0,
        0, 0, 64, 65, 5, 1, 0, 0, 65, 66, 5, 47, 0, 0, 66, 67, 5, 1, 0, 0, 67, 7, 1, 0, 0, 0, 68, 69,
        5, 1, 0, 0, 69, 70, 5, 48, 0, 0, 70, 71, 5, 1, 0, 0, 71, 9, 1, 0, 0, 0, 72, 73, 5, 2, 0, 0,
        73, 74, 5, 47, 0, 0, 74, 75, 5, 2, 0, 0, 75, 11, 1, 0, 0, 0, 76, 77, 5, 2, 0, 0, 77, 78, 5,
        48, 0, 0, 78, 79, 5, 2, 0, 0, 79, 13, 1, 0, 0, 0, 80, 81, 7, 0, 0, 0, 81, 15, 1, 0, 0, 0, 82,
        83, 5, 27, 0, 0, 83, 17, 1, 0, 0, 0, 84, 85, 5, 45, 0, 0, 85, 19, 1, 0, 0, 0, 86, 94, 3, 54,
        27, 0, 87, 88, 3, 54, 27, 0, 88, 89, 5, 9, 0, 0, 89, 94, 1, 0, 0, 0, 90, 91, 3, 54, 27, 0,
        91, 92, 5, 13, 0, 0, 92, 94, 1, 0, 0, 0, 93, 86, 1, 0, 0, 0, 93, 87, 1, 0, 0, 0, 93, 90, 1,
        0, 0, 0, 94, 21, 1, 0, 0, 0, 95, 96, 5, 29, 0, 0, 96, 101, 3, 20, 10, 0, 97, 98, 5, 3, 0,
        0, 98, 100, 3, 20, 10, 0, 99, 97, 1, 0, 0, 0, 100, 103, 1, 0, 0, 0, 101, 99, 1, 0, 0, 0,
        101, 102, 1, 0, 0, 0, 102, 23, 1, 0, 0, 0, 103, 101, 1, 0, 0, 0, 104, 113, 5, 19, 0, 0,
        105, 110, 3, 54, 27, 0, 106, 107, 5, 3, 0, 0, 107, 109, 3, 54, 27, 0, 108, 106, 1, 0,
        0, 0, 109, 112, 1, 0, 0, 0, 110, 108, 1, 0, 0, 0, 110, 111, 1, 0, 0, 0, 111, 114, 1, 0,
        0, 0, 112, 110, 1, 0, 0, 0, 113, 105, 1, 0, 0, 0, 113, 114, 1, 0, 0, 0, 114, 138, 1, 0,
        0, 0, 115, 124, 5, 23, 0, 0, 116, 121, 3, 54, 27, 0, 117, 118, 5, 3, 0, 0, 118, 120, 3,
        54, 27, 0, 119, 117, 1, 0, 0, 0, 120, 123, 1, 0, 0, 0, 121, 119, 1, 0, 0, 0, 121, 122,
        1, 0, 0, 0, 122, 125, 1, 0, 0, 0, 123, 121, 1, 0, 0, 0, 124, 116, 1, 0, 0, 0, 124, 125,
        1, 0, 0, 0, 125, 138, 1, 0, 0, 0, 126, 135, 5, 17, 0, 0, 127, 132, 3, 54, 27, 0, 128, 129,
        5, 3, 0, 0, 129, 131, 3, 54, 27, 0, 130, 128, 1, 0, 0, 0, 131, 134, 1, 0, 0, 0, 132, 130,
        1, 0, 0, 0, 132, 133, 1, 0, 0, 0, 133, 136, 1, 0, 0, 0, 134, 132, 1, 0, 0, 0, 135, 127,
        1, 0, 0, 0, 135, 136, 1, 0, 0, 0, 136, 138, 1, 0, 0, 0, 137, 104, 1, 0, 0, 0, 137, 115,
        1, 0, 0, 0, 137, 126, 1, 0, 0, 0, 138, 25, 1, 0, 0, 0, 139, 163, 5, 31, 0, 0, 140, 149,
        5, 7, 0, 0, 141, 146, 3, 54, 27, 0, 142, 143, 5, 3, 0, 0, 143, 145, 3, 54, 27, 0, 144,
        142, 1, 0, 0, 0, 145, 148, 1, 0, 0, 0, 146, 144, 1, 0, 0, 0, 146, 147, 1, 0, 0, 0, 147,
        150, 1, 0, 0, 0, 148, 146, 1, 0, 0, 0, 149, 141, 1, 0, 0, 0, 149, 150, 1, 0, 0, 0, 150,
        163, 1, 0, 0, 0, 151, 160, 5, 34, 0, 0, 152, 157, 3, 54, 27, 0, 153, 154, 5, 3, 0, 0, 154,
        156, 3, 54, 27, 0, 155, 153, 1, 0, 0, 0, 156, 159, 1, 0, 0, 0, 157, 155, 1, 0, 0, 0, 157,
        158, 1, 0, 0, 0, 158, 161, 1, 0, 0, 0, 159, 157, 1, 0, 0, 0, 160, 152, 1, 0, 0, 0, 160,
        161, 1, 0, 0, 0, 161, 163, 1, 0, 0, 0, 162, 139, 1, 0, 0, 0, 162, 140, 1, 0, 0, 0, 162,
        151, 1, 0, 0, 0, 163, 27, 1, 0, 0, 0, 164, 173, 5, 10, 0, 0, 165, 170, 3, 18, 9, 0, 166,
        167, 5, 3, 0, 0, 167, 169, 3, 18, 9, 0, 168, 166, 1, 0, 0, 0, 169, 172, 1, 0, 0, 0, 170,
        168, 1, 0, 0, 0, 170, 171, 1, 0, 0, 0, 171, 174, 1, 0, 0, 0, 172, 170, 1, 0, 0, 0, 173,
        165, 1, 0, 0, 0, 173, 174, 1, 0, 0, 0, 174, 29, 1, 0, 0, 0, 175, 184, 5, 21, 0, 0, 176,
        181, 3, 18, 9, 0, 177, 178, 5, 3, 0, 0, 178, 180, 3, 18, 9, 0, 179, 177, 1, 0, 0, 0, 180,
        183, 1, 0, 0, 0, 181, 179, 1, 0, 0, 0, 181, 182, 1, 0, 0, 0, 182, 185, 1, 0, 0, 0, 183,
        181, 1, 0, 0, 0, 184, 176, 1, 0, 0, 0, 184, 185, 1, 0, 0, 0, 185, 31, 1, 0, 0, 0, 186, 187,
        7, 1, 0, 0, 187, 196, 5, 40, 0, 0, 188, 193, 3, 54, 27, 0, 189, 190, 5, 3, 0, 0, 190, 192,
        3, 54, 27, 0, 191, 189, 1, 0, 0, 0, 192, 195, 1, 0, 0, 0, 193, 191, 1, 0, 0, 0, 193, 194,
        1, 0, 0, 0, 194, 197, 1, 0, 0, 0, 195, 193, 1, 0, 0, 0, 196, 188, 1, 0, 0, 0, 196, 197,
        1, 0, 0, 0, 197, 199, 1, 0, 0, 0, 198, 200, 3, 26, 13, 0, 199, 198, 1, 0, 0, 0, 199, 200,
        1, 0, 0, 0, 200, 202, 1, 0, 0, 0, 201, 203, 3, 22, 11, 0, 202, 201, 1, 0, 0, 0, 202, 203,
        1, 0, 0, 0, 203, 205, 1, 0, 0, 0, 204, 206, 3, 24, 12, 0, 205, 204, 1, 0, 0, 0, 205, 206,
        1, 0, 0, 0, 206, 208, 1, 0, 0, 0, 207, 209, 3, 28, 14, 0, 208, 207, 1, 0, 0, 0, 208, 209,
        1, 0, 0, 0, 209, 211, 1, 0, 0, 0, 210, 212, 3, 30, 15, 0, 211, 210, 1, 0, 0, 0, 211, 212,
        1, 0, 0, 0, 212, 213, 1, 0, 0, 0, 213, 214, 5, 41, 0, 0, 214, 33, 1, 0, 0, 0, 215, 216,
        5, 15, 0, 0, 216, 217, 3, 54, 27, 0, 217, 218, 5, 30, 0, 0, 218, 219, 3, 54, 27, 0, 219,
        35, 1, 0, 0, 0, 220, 221, 5, 14, 0, 0, 221, 222, 3, 54, 27, 0, 222, 37, 1, 0, 0, 0, 223,
        224, 5, 20, 0, 0, 224, 225, 3, 54, 27, 0, 225, 226, 5, 30, 0, 0, 226, 227, 3, 54, 27,
        0, 227, 39, 1, 0, 0, 0, 228, 232, 3, 38, 19, 0, 229, 231, 3, 34, 17, 0, 230, 229, 1, 0,
        0, 0, 231, 234, 1, 0, 0, 0, 232, 230, 1, 0, 0, 0, 232, 233, 1, 0, 0, 0, 233, 236, 1, 0,
        0, 0, 234, 232, 1, 0, 0, 0, 235, 237, 3, 36, 18, 0, 236, 235, 1, 0, 0, 0, 236, 237, 1,
        0, 0, 0, 237, 238, 1, 0, 0, 0, 238, 239, 5, 16, 0, 0, 239, 41, 1, 0, 0, 0, 240, 241, 5,
        33, 0, 0, 241, 242, 3, 54, 27, 0, 242, 243, 5, 30, 0, 0, 243, 244, 3, 54, 27, 0, 244,
        43, 1, 0, 0, 0, 245, 246, 5, 12, 0, 0, 246, 248, 3, 54, 27, 0, 247, 249, 3, 42, 21, 0,
        248, 247, 1, 0, 0, 0, 249, 250, 1, 0, 0, 0, 250, 248, 1, 0, 0, 0, 250, 251, 1, 0, 0, 0,
        251, 253, 1, 0, 0, 0, 252, 254, 3, 36, 18, 0, 253, 252, 1, 0, 0, 0, 253, 254, 1, 0, 0,
        0, 254, 255, 1, 0, 0, 0, 255, 256, 5, 16, 0, 0, 256, 45, 1, 0, 0, 0, 257, 258, 5, 40, 0,
        0, 258, 259, 3, 54, 27, 0, 259, 260, 5, 41, 0, 0, 260, 47, 1, 0, 0, 0, 261, 276, 3, 0,
        0, 0, 262, 276, 3, 2, 1, 0, 263, 276, 3, 14, 7, 0, 264, 276, 3, 16, 8, 0, 265, 276, 3,
        40, 20, 0, 266, 276, 3, 44, 22, 0, 267, 276, 3, 4, 2, 0, 268, 276, 3, 18, 9, 0, 269, 276,
        3, 6, 3, 0, 270, 276, 3, 8, 4, 0, 271, 276, 3, 10, 5, 0, 272, 276, 3, 12, 6, 0, 273, 276,
        3, 32, 16, 0, 274, 276, 3, 46, 23, 0, 275, 261, 1, 0, 0, 0, 275, 262, 1, 0, 0, 0, 275,
        263, 1, 0, 0, 0, 275, 264, 1, 0, 0, 0, 275, 265, 1, 0, 0, 0, 275, 266, 1, 0, 0, 0, 275,
        267, 1, 0, 0, 0, 275, 268, 1, 0, 0, 0, 275, 269, 1, 0, 0, 0, 275, 270, 1, 0, 0, 0, 275,
        271, 1, 0, 0, 0, 275, 272, 1, 0, 0, 0, 275, 273, 1, 0, 0, 0, 275, 274, 1, 0, 0, 0, 276,
        49, 1, 0, 0, 0, 277, 278, 6, 25, -1, 0, 278, 279, 5, 36, 0, 0, 279, 284, 3, 50, 25, 10,
        280, 281, 5, 26, 0, 0, 281, 284, 3, 50, 25, 2, 282, 284, 3, 48, 24, 0, 283, 277, 1, 0,
        0, 0, 283, 280, 1, 0, 0, 0, 283, 282, 1, 0, 0, 0, 284, 339, 1, 0, 0, 0, 285, 286, 10, 11,
        0, 0, 286, 287, 5, 37, 0, 0, 287, 338, 3, 50, 25, 12, 288, 289, 10, 9, 0, 0, 289, 290,
        5, 38, 0, 0, 290, 338, 3, 50, 25, 10, 291, 292, 10, 8, 0, 0, 292, 293, 7, 2, 0, 0, 293,
        338, 3, 50, 25, 9, 294, 296, 10, 6, 0, 0, 295, 297, 5, 26, 0, 0, 296, 295, 1, 0, 0, 0,
        296, 297, 1, 0, 0, 0, 297, 298, 1, 0, 0, 0, 298, 299, 5, 25, 0, 0, 299, 338, 3, 50, 25,
        7, 300, 301, 10, 5, 0, 0, 301, 302, 5, 39, 0, 0, 302, 338, 3, 50, 25, 6, 303, 307, 10,
        4, 0, 0, 304, 308, 5, 11, 0, 0, 305, 306, 5, 26, 0, 0, 306, 308, 5, 11, 0, 0, 307, 304,
        1, 0, 0, 0, 307, 305, 1, 0, 0, 0, 308, 309, 1, 0, 0, 0, 309, 310, 3, 50, 25, 0, 310, 311,
        5, 8, 0, 0, 311, 312, 3, 50, 25, 5, 312, 338, 1, 0, 0, 0, 313, 314, 10, 7, 0, 0, 314, 316,
        5, 24, 0, 0, 315, 317, 5, 26, 0, 0, 316, 315, 1, 0, 0, 0, 316, 317, 1, 0, 0, 0, 317, 318,
        1, 0, 0, 0, 318, 338, 7, 3, 0, 0, 319, 321, 10, 3, 0, 0, 320, 322, 5, 26, 0, 0, 321, 320,
        1, 0, 0, 0, 321, 322, 1, 0, 0, 0, 322, 323, 1, 0, 0, 0, 323, 324, 5, 22, 0, 0, 324, 334,
        5, 40, 0, 0, 325, 326, 3, 54, 27, 0, 326, 327, 5, 3, 0, 0, 327, 329, 1, 0, 0, 0, 328, 325,
        1, 0, 0, 0, 329, 332, 1, 0, 0, 0, 330, 328, 1, 0, 0, 0, 330, 331, 1, 0, 0, 0, 331, 333,
        1, 0, 0, 0, 332, 330, 1, 0, 0, 0, 333, 335, 3, 54, 27, 0, 334, 330, 1, 0, 0, 0, 334, 335,
        1, 0, 0, 0, 335, 336, 1, 0, 0, 0, 336, 338, 5, 41, 0, 0, 337, 285, 1, 0, 0, 0, 337, 288,
        1, 0, 0, 0, 337, 291, 1, 0, 0, 0, 337, 294, 1, 0, 0, 0, 337, 300, 1, 0, 0, 0, 337, 303,
        1, 0, 0, 0, 337, 313, 1, 0, 0, 0, 337, 319, 1, 0, 0, 0, 338, 341, 1, 0, 0, 0, 339, 337,
        1, 0, 0, 0, 339, 340, 1, 0, 0, 0, 340, 51, 1, 0, 0, 0, 341, 339, 1, 0, 0, 0, 342, 343, 6,
        26, -1, 0, 343, 344, 3, 50, 25, 0, 344, 353, 1, 0, 0, 0, 345, 346, 10, 3, 0, 0, 346, 347,
        5, 8, 0, 0, 347, 352, 3, 52, 26, 4, 348, 349, 10, 2, 0, 0, 349, 350, 5, 28, 0, 0, 350,
        352, 3, 52, 26, 3, 351, 345, 1, 0, 0, 0, 351, 348, 1, 0, 0, 0, 352, 355, 1, 0, 0, 0, 353,
        351, 1, 0, 0, 0, 353, 354, 1, 0, 0, 0, 354, 53, 1, 0, 0, 0, 355, 353, 1, 0, 0, 0, 356, 357,
        3, 52, 26, 0, 357, 55, 1, 0, 0, 0, 358, 359, 3, 54, 27, 0, 359, 360, 5, 0, 0, 1, 360, 363,
        1, 0, 0, 0, 361, 363, 5, 0, 0, 1, 362, 358, 1, 0, 0, 0, 362, 361, 1, 0, 0, 0, 363, 57, 1,
        0, 0, 0, 42, 93, 101, 110, 113, 121, 124, 132, 135, 137, 146, 149, 157, 160, 162,
        170, 173, 181, 184, 193, 196, 199, 202, 205, 208, 211, 232, 236, 250, 253, 275,
        283, 296, 307, 316, 321, 330, 334, 337, 339, 351, 353, 362
    ]


class DataLensParser (Parser):

    grammarFileName = "DataLens.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [DFA(ds, i) for i, ds in enumerate(atn.decisionToState)]

    sharedContextCache = PredictionContextCache()

    literalNames = ["<INVALID>", "'#'", "'##'", "','", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                    "<INVALID>", "'+'", "'-'", "'^'", "<INVALID>", "<INVALID>",
                    "'('", "')'"]

    symbolicNames = ["<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>",
                     "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT", "WS",
                     "AMONG", "AND", "ASC", "BEFORE_FILTER_BY", "BETWEEN",
                     "CASE", "DESC", "ELSE", "ELSEIF", "END", "EXCLUDE",
                     "FALSE", "FIXED", "IF", "IGNORE_DIMENSIONS", "IN",
                     "INCLUDE", "IS", "LIKE", "NOT", "NULL", "OR", "ORDER_BY",
                     "THEN", "TOTAL", "TRUE", "WHEN", "WITHIN", "PLUS",
                     "MINUS", "POWER", "MULDIV", "COMPARISON", "OPENING_PAR",
                     "CLOSING_PAR", "INT", "FLOAT", "ESCAPED_STRING", "FIELD_NAME",
                     "FUNC_NAME", "DATE_INNER", "DATETIME_INNER", "UNEXPECTED_CHARACTER"]

    RULE_integerLiteral = 0
    RULE_floatLiteral = 1
    RULE_stringLiteral = 2
    RULE_dateLiteral = 3
    RULE_datetimeLiteral = 4
    RULE_genericDateLiteral = 5
    RULE_genericDatetimeLiteral = 6
    RULE_boolLiteral = 7
    RULE_nullLiteral = 8
    RULE_fieldName = 9
    RULE_orderingItem = 10
    RULE_ordering = 11
    RULE_lodSpecifier = 12
    RULE_winGrouping = 13
    RULE_beforeFilterBy = 14
    RULE_ignoreDimensions = 15
    RULE_function = 16
    RULE_elseifPart = 17
    RULE_elsePart = 18
    RULE_ifPart = 19
    RULE_ifBlock = 20
    RULE_whenPart = 21
    RULE_caseBlock = 22
    RULE_parenthesizedExpr = 23
    RULE_exprBasic = 24
    RULE_exprMain = 25
    RULE_exprSecondary = 26
    RULE_expression = 27
    RULE_parse = 28

    ruleNames = ["integerLiteral", "floatLiteral", "stringLiteral", "dateLiteral",
                 "datetimeLiteral", "genericDateLiteral", "genericDatetimeLiteral",
                 "boolLiteral", "nullLiteral", "fieldName", "orderingItem",
                 "ordering", "lodSpecifier", "winGrouping", "beforeFilterBy",
                 "ignoreDimensions", "function", "elseifPart", "elsePart",
                 "ifPart", "ifBlock", "whenPart", "caseBlock", "parenthesizedExpr",
                 "exprBasic", "exprMain", "exprSecondary", "expression",
                 "parse"]

    EOF = Token.EOF
    T__0 = 1
    T__1 = 2
    T__2 = 3
    SINGLE_LINE_COMMENT = 4
    MULTI_LINE_COMMENT = 5
    WS = 6
    AMONG = 7
    AND = 8
    ASC = 9
    BEFORE_FILTER_BY = 10
    BETWEEN = 11
    CASE = 12
    DESC = 13
    ELSE = 14
    ELSEIF = 15
    END = 16
    EXCLUDE = 17
    FALSE = 18
    FIXED = 19
    IF = 20
    IGNORE_DIMENSIONS = 21
    IN = 22
    INCLUDE = 23
    IS = 24
    LIKE = 25
    NOT = 26
    NULL = 27
    OR = 28
    ORDER_BY = 29
    THEN = 30
    TOTAL = 31
    TRUE = 32
    WHEN = 33
    WITHIN = 34
    PLUS = 35
    MINUS = 36
    POWER = 37
    MULDIV = 38
    COMPARISON = 39
    OPENING_PAR = 40
    CLOSING_PAR = 41
    INT = 42
    FLOAT = 43
    ESCAPED_STRING = 44
    FIELD_NAME = 45
    FUNC_NAME = 46
    DATE_INNER = 47
    DATETIME_INNER = 48
    UNEXPECTED_CHARACTER = 49

    def __init__(self, input: TokenStream, output: TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.11.1")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None

    class IntegerLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def INT(self):
            return self.getToken(DataLensParser.INT, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_integerLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitIntegerLiteral"):
                return visitor.visitIntegerLiteral(self)
            else:
                return visitor.visitChildren(self)

    def integerLiteral(self):

        localctx = DataLensParser.IntegerLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_integerLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 58
            self.match(DataLensParser.INT)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FloatLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def FLOAT(self):
            return self.getToken(DataLensParser.FLOAT, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_floatLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitFloatLiteral"):
                return visitor.visitFloatLiteral(self)
            else:
                return visitor.visitChildren(self)

    def floatLiteral(self):

        localctx = DataLensParser.FloatLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_floatLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 60
            self.match(DataLensParser.FLOAT)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class StringLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ESCAPED_STRING(self):
            return self.getToken(DataLensParser.ESCAPED_STRING, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_stringLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitStringLiteral"):
                return visitor.visitStringLiteral(self)
            else:
                return visitor.visitChildren(self)

    def stringLiteral(self):

        localctx = DataLensParser.StringLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_stringLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 62
            self.match(DataLensParser.ESCAPED_STRING)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class DateLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DATE_INNER(self):
            return self.getToken(DataLensParser.DATE_INNER, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_dateLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitDateLiteral"):
                return visitor.visitDateLiteral(self)
            else:
                return visitor.visitChildren(self)

    def dateLiteral(self):

        localctx = DataLensParser.DateLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_dateLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 64
            self.match(DataLensParser.T__0)
            self.state = 65
            self.match(DataLensParser.DATE_INNER)
            self.state = 66
            self.match(DataLensParser.T__0)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class DatetimeLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DATETIME_INNER(self):
            return self.getToken(DataLensParser.DATETIME_INNER, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_datetimeLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitDatetimeLiteral"):
                return visitor.visitDatetimeLiteral(self)
            else:
                return visitor.visitChildren(self)

    def datetimeLiteral(self):

        localctx = DataLensParser.DatetimeLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_datetimeLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 68
            self.match(DataLensParser.T__0)
            self.state = 69
            self.match(DataLensParser.DATETIME_INNER)
            self.state = 70
            self.match(DataLensParser.T__0)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class GenericDateLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DATE_INNER(self):
            return self.getToken(DataLensParser.DATE_INNER, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_genericDateLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitGenericDateLiteral"):
                return visitor.visitGenericDateLiteral(self)
            else:
                return visitor.visitChildren(self)

    def genericDateLiteral(self):

        localctx = DataLensParser.GenericDateLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 10, self.RULE_genericDateLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 72
            self.match(DataLensParser.T__1)
            self.state = 73
            self.match(DataLensParser.DATE_INNER)
            self.state = 74
            self.match(DataLensParser.T__1)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class GenericDatetimeLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DATETIME_INNER(self):
            return self.getToken(DataLensParser.DATETIME_INNER, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_genericDatetimeLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitGenericDatetimeLiteral"):
                return visitor.visitGenericDatetimeLiteral(self)
            else:
                return visitor.visitChildren(self)

    def genericDatetimeLiteral(self):

        localctx = DataLensParser.GenericDatetimeLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 12, self.RULE_genericDatetimeLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 76
            self.match(DataLensParser.T__1)
            self.state = 77
            self.match(DataLensParser.DATETIME_INNER)
            self.state = 78
            self.match(DataLensParser.T__1)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class BoolLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def TRUE(self):
            return self.getToken(DataLensParser.TRUE, 0)

        def FALSE(self):
            return self.getToken(DataLensParser.FALSE, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_boolLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitBoolLiteral"):
                return visitor.visitBoolLiteral(self)
            else:
                return visitor.visitChildren(self)

    def boolLiteral(self):

        localctx = DataLensParser.BoolLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 14, self.RULE_boolLiteral)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 80
            _la = self._input.LA(1)
            if not (_la == 18 or _la == 32):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class NullLiteralContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def NULL(self):
            return self.getToken(DataLensParser.NULL, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_nullLiteral

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitNullLiteral"):
                return visitor.visitNullLiteral(self)
            else:
                return visitor.visitChildren(self)

    def nullLiteral(self):

        localctx = DataLensParser.NullLiteralContext(self, self._ctx, self.state)
        self.enterRule(localctx, 16, self.RULE_nullLiteral)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 82
            self.match(DataLensParser.NULL)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FieldNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def FIELD_NAME(self):
            return self.getToken(DataLensParser.FIELD_NAME, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_fieldName

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitFieldName"):
                return visitor.visitFieldName(self)
            else:
                return visitor.visitChildren(self)

    def fieldName(self):

        localctx = DataLensParser.FieldNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 18, self.RULE_fieldName)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 84
            self.match(DataLensParser.FIELD_NAME)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class OrderingItemContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def expression(self):
            return self.getTypedRuleContext(DataLensParser.ExpressionContext, 0)

        def ASC(self):
            return self.getToken(DataLensParser.ASC, 0)

        def DESC(self):
            return self.getToken(DataLensParser.DESC, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_orderingItem

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitOrderingItem"):
                return visitor.visitOrderingItem(self)
            else:
                return visitor.visitChildren(self)

    def orderingItem(self):

        localctx = DataLensParser.OrderingItemContext(self, self._ctx, self.state)
        self.enterRule(localctx, 20, self.RULE_orderingItem)
        try:
            self.state = 93
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input, 0, self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 86
                self.expression()

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 87
                self.expression()
                self.state = 88
                self.match(DataLensParser.ASC)

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 90
                self.expression()
                self.state = 91
                self.match(DataLensParser.DESC)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class OrderingContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ORDER_BY(self):
            return self.getToken(DataLensParser.ORDER_BY, 0)

        def orderingItem(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.OrderingItemContext)
            else:
                return self.getTypedRuleContext(DataLensParser.OrderingItemContext, i)

        def getRuleIndex(self):
            return DataLensParser.RULE_ordering

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitOrdering"):
                return visitor.visitOrdering(self)
            else:
                return visitor.visitChildren(self)

    def ordering(self):

        localctx = DataLensParser.OrderingContext(self, self._ctx, self.state)
        self.enterRule(localctx, 22, self.RULE_ordering)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 95
            self.match(DataLensParser.ORDER_BY)
            self.state = 96
            self.orderingItem()
            self.state = 101
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la == 3:
                self.state = 97
                self.match(DataLensParser.T__2)
                self.state = 98
                self.orderingItem()
                self.state = 103
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class LodSpecifierContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def FIXED(self):
            return self.getToken(DataLensParser.FIXED, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def INCLUDE(self):
            return self.getToken(DataLensParser.INCLUDE, 0)

        def EXCLUDE(self):
            return self.getToken(DataLensParser.EXCLUDE, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_lodSpecifier

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitLodSpecifier"):
                return visitor.visitLodSpecifier(self)
            else:
                return visitor.visitChildren(self)

    def lodSpecifier(self):

        localctx = DataLensParser.LodSpecifierContext(self, self._ctx, self.state)
        self.enterRule(localctx, 24, self.RULE_lodSpecifier)
        self._la = 0  # Token type
        try:
            self.state = 137
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [19]:
                self.enterOuterAlt(localctx, 1)
                self.state = 104
                self.match(DataLensParser.FIXED)
                self.state = 113
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                    self.state = 105
                    self.expression()
                    self.state = 110
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la == 3:
                        self.state = 106
                        self.match(DataLensParser.T__2)
                        self.state = 107
                        self.expression()
                        self.state = 112
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

            elif token in [23]:
                self.enterOuterAlt(localctx, 2)
                self.state = 115
                self.match(DataLensParser.INCLUDE)
                self.state = 124
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                    self.state = 116
                    self.expression()
                    self.state = 121
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la == 3:
                        self.state = 117
                        self.match(DataLensParser.T__2)
                        self.state = 118
                        self.expression()
                        self.state = 123
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

            elif token in [17]:
                self.enterOuterAlt(localctx, 3)
                self.state = 126
                self.match(DataLensParser.EXCLUDE)
                self.state = 135
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                    self.state = 127
                    self.expression()
                    self.state = 132
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la == 3:
                        self.state = 128
                        self.match(DataLensParser.T__2)
                        self.state = 129
                        self.expression()
                        self.state = 134
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WinGroupingContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def TOTAL(self):
            return self.getToken(DataLensParser.TOTAL, 0)

        def AMONG(self):
            return self.getToken(DataLensParser.AMONG, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def WITHIN(self):
            return self.getToken(DataLensParser.WITHIN, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_winGrouping

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitWinGrouping"):
                return visitor.visitWinGrouping(self)
            else:
                return visitor.visitChildren(self)

    def winGrouping(self):

        localctx = DataLensParser.WinGroupingContext(self, self._ctx, self.state)
        self.enterRule(localctx, 26, self.RULE_winGrouping)
        self._la = 0  # Token type
        try:
            self.state = 162
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [31]:
                self.enterOuterAlt(localctx, 1)
                self.state = 139
                self.match(DataLensParser.TOTAL)
            elif token in [7]:
                self.enterOuterAlt(localctx, 2)
                self.state = 140
                self.match(DataLensParser.AMONG)
                self.state = 149
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                    self.state = 141
                    self.expression()
                    self.state = 146
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la == 3:
                        self.state = 142
                        self.match(DataLensParser.T__2)
                        self.state = 143
                        self.expression()
                        self.state = 148
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

            elif token in [34]:
                self.enterOuterAlt(localctx, 3)
                self.state = 151
                self.match(DataLensParser.WITHIN)
                self.state = 160
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                    self.state = 152
                    self.expression()
                    self.state = 157
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)
                    while _la == 3:
                        self.state = 153
                        self.match(DataLensParser.T__2)
                        self.state = 154
                        self.expression()
                        self.state = 159
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)

            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class BeforeFilterByContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def BEFORE_FILTER_BY(self):
            return self.getToken(DataLensParser.BEFORE_FILTER_BY, 0)

        def fieldName(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.FieldNameContext)
            else:
                return self.getTypedRuleContext(DataLensParser.FieldNameContext, i)

        def getRuleIndex(self):
            return DataLensParser.RULE_beforeFilterBy

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitBeforeFilterBy"):
                return visitor.visitBeforeFilterBy(self)
            else:
                return visitor.visitChildren(self)

    def beforeFilterBy(self):

        localctx = DataLensParser.BeforeFilterByContext(self, self._ctx, self.state)
        self.enterRule(localctx, 28, self.RULE_beforeFilterBy)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 164
            self.match(DataLensParser.BEFORE_FILTER_BY)
            self.state = 173
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 45:
                self.state = 165
                self.fieldName()
                self.state = 170
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la == 3:
                    self.state = 166
                    self.match(DataLensParser.T__2)
                    self.state = 167
                    self.fieldName()
                    self.state = 172
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IgnoreDimensionsContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def IGNORE_DIMENSIONS(self):
            return self.getToken(DataLensParser.IGNORE_DIMENSIONS, 0)

        def fieldName(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.FieldNameContext)
            else:
                return self.getTypedRuleContext(DataLensParser.FieldNameContext, i)

        def getRuleIndex(self):
            return DataLensParser.RULE_ignoreDimensions

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitIgnoreDimensions"):
                return visitor.visitIgnoreDimensions(self)
            else:
                return visitor.visitChildren(self)

    def ignoreDimensions(self):

        localctx = DataLensParser.IgnoreDimensionsContext(self, self._ctx, self.state)
        self.enterRule(localctx, 30, self.RULE_ignoreDimensions)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 175
            self.match(DataLensParser.IGNORE_DIMENSIONS)
            self.state = 184
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 45:
                self.state = 176
                self.fieldName()
                self.state = 181
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la == 3:
                    self.state = 177
                    self.match(DataLensParser.T__2)
                    self.state = 178
                    self.fieldName()
                    self.state = 183
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class FunctionContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def OPENING_PAR(self):
            return self.getToken(DataLensParser.OPENING_PAR, 0)

        def CLOSING_PAR(self):
            return self.getToken(DataLensParser.CLOSING_PAR, 0)

        def FUNC_NAME(self):
            return self.getToken(DataLensParser.FUNC_NAME, 0)

        def CASE(self):
            return self.getToken(DataLensParser.CASE, 0)

        def IF(self):
            return self.getToken(DataLensParser.IF, 0)

        def NOT(self):
            return self.getToken(DataLensParser.NOT, 0)

        def AND(self):
            return self.getToken(DataLensParser.AND, 0)

        def OR(self):
            return self.getToken(DataLensParser.OR, 0)

        def LIKE(self):
            return self.getToken(DataLensParser.LIKE, 0)

        def BETWEEN(self):
            return self.getToken(DataLensParser.BETWEEN, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def winGrouping(self):
            return self.getTypedRuleContext(DataLensParser.WinGroupingContext, 0)

        def ordering(self):
            return self.getTypedRuleContext(DataLensParser.OrderingContext, 0)

        def lodSpecifier(self):
            return self.getTypedRuleContext(DataLensParser.LodSpecifierContext, 0)

        def beforeFilterBy(self):
            return self.getTypedRuleContext(DataLensParser.BeforeFilterByContext, 0)

        def ignoreDimensions(self):
            return self.getTypedRuleContext(DataLensParser.IgnoreDimensionsContext, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_function

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitFunction"):
                return visitor.visitFunction(self)
            else:
                return visitor.visitChildren(self)

    def function(self):

        localctx = DataLensParser.FunctionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 32, self.RULE_function)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 186
            _la = self._input.LA(1)
            if not (((_la) & ~0x3f) == 0 and ((1 << _la) & 70369114331392) != 0):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
            self.state = 187
            self.match(DataLensParser.OPENING_PAR)
            self.state = 196
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                self.state = 188
                self.expression()
                self.state = 193
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la == 3:
                    self.state = 189
                    self.match(DataLensParser.T__2)
                    self.state = 190
                    self.expression()
                    self.state = 195
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)

            self.state = 199
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 19327352960) != 0:
                self.state = 198
                self.winGrouping()

            self.state = 202
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 29:
                self.state = 201
                self.ordering()

            self.state = 205
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if ((_la) & ~0x3f) == 0 and ((1 << _la) & 9043968) != 0:
                self.state = 204
                self.lodSpecifier()

            self.state = 208
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 10:
                self.state = 207
                self.beforeFilterBy()

            self.state = 211
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 21:
                self.state = 210
                self.ignoreDimensions()

            self.state = 213
            self.match(DataLensParser.CLOSING_PAR)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ElseifPartContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ELSEIF(self):
            return self.getToken(DataLensParser.ELSEIF, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def THEN(self):
            return self.getToken(DataLensParser.THEN, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_elseifPart

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitElseifPart"):
                return visitor.visitElseifPart(self)
            else:
                return visitor.visitChildren(self)

    def elseifPart(self):

        localctx = DataLensParser.ElseifPartContext(self, self._ctx, self.state)
        self.enterRule(localctx, 34, self.RULE_elseifPart)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 215
            self.match(DataLensParser.ELSEIF)
            self.state = 216
            self.expression()
            self.state = 217
            self.match(DataLensParser.THEN)
            self.state = 218
            self.expression()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ElsePartContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ELSE(self):
            return self.getToken(DataLensParser.ELSE, 0)

        def expression(self):
            return self.getTypedRuleContext(DataLensParser.ExpressionContext, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_elsePart

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitElsePart"):
                return visitor.visitElsePart(self)
            else:
                return visitor.visitChildren(self)

    def elsePart(self):

        localctx = DataLensParser.ElsePartContext(self, self._ctx, self.state)
        self.enterRule(localctx, 36, self.RULE_elsePart)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 220
            self.match(DataLensParser.ELSE)
            self.state = 221
            self.expression()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IfPartContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def IF(self):
            return self.getToken(DataLensParser.IF, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def THEN(self):
            return self.getToken(DataLensParser.THEN, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_ifPart

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitIfPart"):
                return visitor.visitIfPart(self)
            else:
                return visitor.visitChildren(self)

    def ifPart(self):

        localctx = DataLensParser.IfPartContext(self, self._ctx, self.state)
        self.enterRule(localctx, 38, self.RULE_ifPart)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 223
            self.match(DataLensParser.IF)
            self.state = 224
            self.expression()
            self.state = 225
            self.match(DataLensParser.THEN)
            self.state = 226
            self.expression()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class IfBlockContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ifPart(self):
            return self.getTypedRuleContext(DataLensParser.IfPartContext, 0)

        def END(self):
            return self.getToken(DataLensParser.END, 0)

        def elseifPart(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ElseifPartContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ElseifPartContext, i)

        def elsePart(self):
            return self.getTypedRuleContext(DataLensParser.ElsePartContext, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_ifBlock

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitIfBlock"):
                return visitor.visitIfBlock(self)
            else:
                return visitor.visitChildren(self)

    def ifBlock(self):

        localctx = DataLensParser.IfBlockContext(self, self._ctx, self.state)
        self.enterRule(localctx, 40, self.RULE_ifBlock)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 228
            self.ifPart()
            self.state = 232
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la == 15:
                self.state = 229
                self.elseifPart()
                self.state = 234
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 236
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 14:
                self.state = 235
                self.elsePart()

            self.state = 238
            self.match(DataLensParser.END)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class WhenPartContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def WHEN(self):
            return self.getToken(DataLensParser.WHEN, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def THEN(self):
            return self.getToken(DataLensParser.THEN, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_whenPart

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitWhenPart"):
                return visitor.visitWhenPart(self)
            else:
                return visitor.visitChildren(self)

    def whenPart(self):

        localctx = DataLensParser.WhenPartContext(self, self._ctx, self.state)
        self.enterRule(localctx, 42, self.RULE_whenPart)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 240
            self.match(DataLensParser.WHEN)
            self.state = 241
            self.expression()
            self.state = 242
            self.match(DataLensParser.THEN)
            self.state = 243
            self.expression()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class CaseBlockContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def CASE(self):
            return self.getToken(DataLensParser.CASE, 0)

        def expression(self):
            return self.getTypedRuleContext(DataLensParser.ExpressionContext, 0)

        def END(self):
            return self.getToken(DataLensParser.END, 0)

        def whenPart(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.WhenPartContext)
            else:
                return self.getTypedRuleContext(DataLensParser.WhenPartContext, i)

        def elsePart(self):
            return self.getTypedRuleContext(DataLensParser.ElsePartContext, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_caseBlock

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitCaseBlock"):
                return visitor.visitCaseBlock(self)
            else:
                return visitor.visitChildren(self)

    def caseBlock(self):

        localctx = DataLensParser.CaseBlockContext(self, self._ctx, self.state)
        self.enterRule(localctx, 44, self.RULE_caseBlock)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 245
            self.match(DataLensParser.CASE)
            self.state = 246
            self.expression()
            self.state = 248
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 247
                self.whenPart()
                self.state = 250
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (_la == 33):
                    break

            self.state = 253
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la == 14:
                self.state = 252
                self.elsePart()

            self.state = 255
            self.match(DataLensParser.END)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ParenthesizedExprContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def OPENING_PAR(self):
            return self.getToken(DataLensParser.OPENING_PAR, 0)

        def expression(self):
            return self.getTypedRuleContext(DataLensParser.ExpressionContext, 0)

        def CLOSING_PAR(self):
            return self.getToken(DataLensParser.CLOSING_PAR, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_parenthesizedExpr

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitParenthesizedExpr"):
                return visitor.visitParenthesizedExpr(self)
            else:
                return visitor.visitChildren(self)

    def parenthesizedExpr(self):

        localctx = DataLensParser.ParenthesizedExprContext(self, self._ctx, self.state)
        self.enterRule(localctx, 46, self.RULE_parenthesizedExpr)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 257
            self.match(DataLensParser.OPENING_PAR)
            self.state = 258
            self.expression()
            self.state = 259
            self.match(DataLensParser.CLOSING_PAR)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ExprBasicContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def integerLiteral(self):
            return self.getTypedRuleContext(DataLensParser.IntegerLiteralContext, 0)

        def floatLiteral(self):
            return self.getTypedRuleContext(DataLensParser.FloatLiteralContext, 0)

        def boolLiteral(self):
            return self.getTypedRuleContext(DataLensParser.BoolLiteralContext, 0)

        def nullLiteral(self):
            return self.getTypedRuleContext(DataLensParser.NullLiteralContext, 0)

        def ifBlock(self):
            return self.getTypedRuleContext(DataLensParser.IfBlockContext, 0)

        def caseBlock(self):
            return self.getTypedRuleContext(DataLensParser.CaseBlockContext, 0)

        def stringLiteral(self):
            return self.getTypedRuleContext(DataLensParser.StringLiteralContext, 0)

        def fieldName(self):
            return self.getTypedRuleContext(DataLensParser.FieldNameContext, 0)

        def dateLiteral(self):
            return self.getTypedRuleContext(DataLensParser.DateLiteralContext, 0)

        def datetimeLiteral(self):
            return self.getTypedRuleContext(DataLensParser.DatetimeLiteralContext, 0)

        def genericDateLiteral(self):
            return self.getTypedRuleContext(DataLensParser.GenericDateLiteralContext, 0)

        def genericDatetimeLiteral(self):
            return self.getTypedRuleContext(DataLensParser.GenericDatetimeLiteralContext, 0)

        def function(self):
            return self.getTypedRuleContext(DataLensParser.FunctionContext, 0)

        def parenthesizedExpr(self):
            return self.getTypedRuleContext(DataLensParser.ParenthesizedExprContext, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_exprBasic

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitExprBasic"):
                return visitor.visitExprBasic(self)
            else:
                return visitor.visitChildren(self)

    def exprBasic(self):

        localctx = DataLensParser.ExprBasicContext(self, self._ctx, self.state)
        self.enterRule(localctx, 48, self.RULE_exprBasic)
        try:
            self.state = 275
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input, 29, self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 261
                self.integerLiteral()

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 262
                self.floatLiteral()

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 263
                self.boolLiteral()

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 264
                self.nullLiteral()

            elif la_ == 5:
                self.enterOuterAlt(localctx, 5)
                self.state = 265
                self.ifBlock()

            elif la_ == 6:
                self.enterOuterAlt(localctx, 6)
                self.state = 266
                self.caseBlock()

            elif la_ == 7:
                self.enterOuterAlt(localctx, 7)
                self.state = 267
                self.stringLiteral()

            elif la_ == 8:
                self.enterOuterAlt(localctx, 8)
                self.state = 268
                self.fieldName()

            elif la_ == 9:
                self.enterOuterAlt(localctx, 9)
                self.state = 269
                self.dateLiteral()

            elif la_ == 10:
                self.enterOuterAlt(localctx, 10)
                self.state = 270
                self.datetimeLiteral()

            elif la_ == 11:
                self.enterOuterAlt(localctx, 11)
                self.state = 271
                self.genericDateLiteral()

            elif la_ == 12:
                self.enterOuterAlt(localctx, 12)
                self.state = 272
                self.genericDatetimeLiteral()

            elif la_ == 13:
                self.enterOuterAlt(localctx, 13)
                self.state = 273
                self.function()

            elif la_ == 14:
                self.enterOuterAlt(localctx, 14)
                self.state = 274
                self.parenthesizedExpr()

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ExprMainContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def getRuleIndex(self):
            return DataLensParser.RULE_exprMain

        def copyFrom(self, ctx: ParserRuleContext):
            super().copyFrom(ctx)

    class ExprBasicAltContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprBasic(self):
            return self.getTypedRuleContext(DataLensParser.ExprBasicContext, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitExprBasicAlt"):
                return visitor.visitExprBasicAlt(self)
            else:
                return visitor.visitChildren(self)

    class UnaryPrefixContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def MINUS(self):
            return self.getToken(DataLensParser.MINUS, 0)

        def exprMain(self):
            return self.getTypedRuleContext(DataLensParser.ExprMainContext, 0)

        def NOT(self):
            return self.getToken(DataLensParser.NOT, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitUnaryPrefix"):
                return visitor.visitUnaryPrefix(self)
            else:
                return visitor.visitChildren(self)

    class InExprContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprMain(self):
            return self.getTypedRuleContext(DataLensParser.ExprMainContext, 0)

        def IN(self):
            return self.getToken(DataLensParser.IN, 0)

        def OPENING_PAR(self):
            return self.getToken(DataLensParser.OPENING_PAR, 0)

        def CLOSING_PAR(self):
            return self.getToken(DataLensParser.CLOSING_PAR, 0)

        def NOT(self):
            return self.getToken(DataLensParser.NOT, 0)

        def expression(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExpressionContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExpressionContext, i)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitInExpr"):
                return visitor.visitInExpr(self)
            else:
                return visitor.visitChildren(self)

    class BinaryExprContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprMain(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExprMainContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExprMainContext, i)

        def POWER(self):
            return self.getToken(DataLensParser.POWER, 0)

        def MULDIV(self):
            return self.getToken(DataLensParser.MULDIV, 0)

        def PLUS(self):
            return self.getToken(DataLensParser.PLUS, 0)

        def MINUS(self):
            return self.getToken(DataLensParser.MINUS, 0)

        def LIKE(self):
            return self.getToken(DataLensParser.LIKE, 0)

        def NOT(self):
            return self.getToken(DataLensParser.NOT, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitBinaryExpr"):
                return visitor.visitBinaryExpr(self)
            else:
                return visitor.visitChildren(self)

    class ComparisonChainContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprMain(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExprMainContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExprMainContext, i)

        def COMPARISON(self):
            return self.getToken(DataLensParser.COMPARISON, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitComparisonChain"):
                return visitor.visitComparisonChain(self)
            else:
                return visitor.visitChildren(self)

    class UnaryPostfixContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprMain(self):
            return self.getTypedRuleContext(DataLensParser.ExprMainContext, 0)

        def IS(self):
            return self.getToken(DataLensParser.IS, 0)

        def TRUE(self):
            return self.getToken(DataLensParser.TRUE, 0)

        def FALSE(self):
            return self.getToken(DataLensParser.FALSE, 0)

        def NULL(self):
            return self.getToken(DataLensParser.NULL, 0)

        def NOT(self):
            return self.getToken(DataLensParser.NOT, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitUnaryPostfix"):
                return visitor.visitUnaryPostfix(self)
            else:
                return visitor.visitChildren(self)

    class BetweenExprContext(ExprMainContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprMainContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprMain(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExprMainContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExprMainContext, i)

        def AND(self):
            return self.getToken(DataLensParser.AND, 0)

        def BETWEEN(self):
            return self.getToken(DataLensParser.BETWEEN, 0)

        def NOT(self):
            return self.getToken(DataLensParser.NOT, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitBetweenExpr"):
                return visitor.visitBetweenExpr(self)
            else:
                return visitor.visitChildren(self)

    def exprMain(self, _p: int = 0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = DataLensParser.ExprMainContext(self, self._ctx, _parentState)

        _startState = 50
        self.enterRecursionRule(localctx, 50, self.RULE_exprMain, _p)
        self._la = 0  # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 283
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input, 30, self._ctx)
            if la_ == 1:
                localctx = DataLensParser.UnaryPrefixContext(self, localctx)
                self._ctx = localctx

                self.state = 278
                self.match(DataLensParser.MINUS)
                self.state = 279
                self.exprMain(10)

            elif la_ == 2:
                localctx = DataLensParser.UnaryPrefixContext(self, localctx)
                self._ctx = localctx

                self.state = 280
                self.match(DataLensParser.NOT)
                self.state = 281
                self.exprMain(2)

            elif la_ == 3:
                localctx = DataLensParser.ExprBasicAltContext(self, localctx)
                self._ctx = localctx

                self.state = 282
                self.exprBasic()

            self._ctx.stop = self._input.LT(-1)
            self.state = 339
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input, 38, self._ctx)
            while _alt != 2 and _alt != ATN.INVALID_ALT_NUMBER:
                if _alt == 1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()

                    self.state = 337
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input, 37, self._ctx)
                    if la_ == 1:
                        localctx = DataLensParser.BinaryExprContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 285
                        if not self.precpred(self._ctx, 11):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 11)")
                        self.state = 286
                        self.match(DataLensParser.POWER)
                        self.state = 287
                        self.exprMain(12)

                    elif la_ == 2:
                        localctx = DataLensParser.BinaryExprContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 288
                        if not self.precpred(self._ctx, 9):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 9)")
                        self.state = 289
                        self.match(DataLensParser.MULDIV)
                        self.state = 290
                        self.exprMain(10)

                    elif la_ == 3:
                        localctx = DataLensParser.BinaryExprContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 291
                        if not self.precpred(self._ctx, 8):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 8)")
                        self.state = 292
                        _la = self._input.LA(1)
                        if not (_la == 35 or _la == 36):
                            self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()
                        self.state = 293
                        self.exprMain(9)

                    elif la_ == 4:
                        localctx = DataLensParser.BinaryExprContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 294
                        if not self.precpred(self._ctx, 6):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 6)")
                        self.state = 296
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if _la == 26:
                            self.state = 295
                            self.match(DataLensParser.NOT)

                        self.state = 298
                        self.match(DataLensParser.LIKE)
                        self.state = 299
                        self.exprMain(7)

                    elif la_ == 5:
                        localctx = DataLensParser.ComparisonChainContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 300
                        if not self.precpred(self._ctx, 5):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 5)")
                        self.state = 301
                        self.match(DataLensParser.COMPARISON)
                        self.state = 302
                        self.exprMain(6)

                    elif la_ == 6:
                        localctx = DataLensParser.BetweenExprContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 303
                        if not self.precpred(self._ctx, 4):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 4)")
                        self.state = 307
                        self._errHandler.sync(self)
                        token = self._input.LA(1)
                        if token in [11]:
                            self.state = 304
                            self.match(DataLensParser.BETWEEN)
                        elif token in [26]:
                            self.state = 305
                            self.match(DataLensParser.NOT)
                            self.state = 306
                            self.match(DataLensParser.BETWEEN)
                        else:
                            raise NoViableAltException(self)

                        self.state = 309
                        self.exprMain(0)
                        self.state = 310
                        self.match(DataLensParser.AND)
                        self.state = 311
                        self.exprMain(5)

                    elif la_ == 7:
                        localctx = DataLensParser.UnaryPostfixContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 313
                        if not self.precpred(self._ctx, 7):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 7)")
                        self.state = 314
                        self.match(DataLensParser.IS)
                        self.state = 316
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if _la == 26:
                            self.state = 315
                            self.match(DataLensParser.NOT)

                        self.state = 318
                        _la = self._input.LA(1)
                        if not (((_la) & ~0x3f) == 0 and ((1 << _la) & 4429447168) != 0):
                            self._errHandler.recoverInline(self)
                        else:
                            self._errHandler.reportMatch(self)
                            self.consume()

                    elif la_ == 8:
                        localctx = DataLensParser.InExprContext(self, DataLensParser.ExprMainContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprMain)
                        self.state = 319
                        if not self.precpred(self._ctx, 3):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 3)")
                        self.state = 321
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if _la == 26:
                            self.state = 320
                            self.match(DataLensParser.NOT)

                        self.state = 323
                        self.match(DataLensParser.IN)
                        self.state = 324
                        self.match(DataLensParser.OPENING_PAR)
                        self.state = 334
                        self._errHandler.sync(self)
                        _la = self._input.LA(1)
                        if ((_la) & ~0x3f) == 0 and ((1 << _la) & 137512472549638) != 0:
                            self.state = 330
                            self._errHandler.sync(self)
                            _alt = self._interp.adaptivePredict(self._input, 35, self._ctx)
                            while _alt != 2 and _alt != ATN.INVALID_ALT_NUMBER:
                                if _alt == 1:
                                    self.state = 325
                                    self.expression()
                                    self.state = 326
                                    self.match(DataLensParser.T__2)
                                self.state = 332
                                self._errHandler.sync(self)
                                _alt = self._interp.adaptivePredict(self._input, 35, self._ctx)

                            self.state = 333
                            self.expression()

                        self.state = 336
                        self.match(DataLensParser.CLOSING_PAR)

                self.state = 341
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input, 38, self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class ExprSecondaryContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def getRuleIndex(self):
            return DataLensParser.RULE_exprSecondary

        def copyFrom(self, ctx: ParserRuleContext):
            super().copyFrom(ctx)

    class BinaryExprSecContext(ExprSecondaryContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprSecondaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprSecondary(self, i: int = None):
            if i is None:
                return self.getTypedRuleContexts(DataLensParser.ExprSecondaryContext)
            else:
                return self.getTypedRuleContext(DataLensParser.ExprSecondaryContext, i)

        def AND(self):
            return self.getToken(DataLensParser.AND, 0)

        def OR(self):
            return self.getToken(DataLensParser.OR, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitBinaryExprSec"):
                return visitor.visitBinaryExprSec(self)
            else:
                return visitor.visitChildren(self)

    class ExprMainAltContext(ExprSecondaryContext):

        def __init__(self, parser, ctx: ParserRuleContext):  # actually a DataLensParser.ExprSecondaryContext
            super().__init__(parser)
            self.copyFrom(ctx)

        def exprMain(self):
            return self.getTypedRuleContext(DataLensParser.ExprMainContext, 0)

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitExprMainAlt"):
                return visitor.visitExprMainAlt(self)
            else:
                return visitor.visitChildren(self)

    def exprSecondary(self, _p: int = 0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = DataLensParser.ExprSecondaryContext(self, self._ctx, _parentState)

        _startState = 52
        self.enterRecursionRule(localctx, 52, self.RULE_exprSecondary, _p)
        try:
            self.enterOuterAlt(localctx, 1)
            localctx = DataLensParser.ExprMainAltContext(self, localctx)
            self._ctx = localctx

            self.state = 343
            self.exprMain(0)
            self._ctx.stop = self._input.LT(-1)
            self.state = 353
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input, 40, self._ctx)
            while _alt != 2 and _alt != ATN.INVALID_ALT_NUMBER:
                if _alt == 1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()

                    self.state = 351
                    self._errHandler.sync(self)
                    la_ = self._interp.adaptivePredict(self._input, 39, self._ctx)
                    if la_ == 1:
                        localctx = DataLensParser.BinaryExprSecContext(self, DataLensParser.ExprSecondaryContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprSecondary)
                        self.state = 345
                        if not self.precpred(self._ctx, 3):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 3)")
                        self.state = 346
                        self.match(DataLensParser.AND)
                        self.state = 347
                        self.exprSecondary(4)

                    elif la_ == 2:
                        localctx = DataLensParser.BinaryExprSecContext(self, DataLensParser.ExprSecondaryContext(self, _parentctx, _parentState))
                        self.pushNewRecursionContext(localctx, _startState, self.RULE_exprSecondary)
                        self.state = 348
                        if not self.precpred(self._ctx, 2):
                            from antlr4.error.Errors import FailedPredicateException
                            raise FailedPredicateException(self, "self.precpred(self._ctx, 2)")
                        self.state = 349
                        self.match(DataLensParser.OR)
                        self.state = 350
                        self.exprSecondary(3)

                self.state = 355
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input, 40, self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx

    class ExpressionContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def exprSecondary(self):
            return self.getTypedRuleContext(DataLensParser.ExprSecondaryContext, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_expression

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitExpression"):
                return visitor.visitExpression(self)
            else:
                return visitor.visitChildren(self)

    def expression(self):

        localctx = DataLensParser.ExpressionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 54, self.RULE_expression)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 356
            self.exprSecondary(0)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class ParseContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent: ParserRuleContext = None, invokingState: int = -1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def expression(self):
            return self.getTypedRuleContext(DataLensParser.ExpressionContext, 0)

        def EOF(self):
            return self.getToken(DataLensParser.EOF, 0)

        def getRuleIndex(self):
            return DataLensParser.RULE_parse

        def accept(self, visitor: ParseTreeVisitor):
            if hasattr(visitor, "visitParse"):
                return visitor.visitParse(self)
            else:
                return visitor.visitChildren(self)

    def parse(self):

        localctx = DataLensParser.ParseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 56, self.RULE_parse)
        try:
            self.state = 362
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [1, 2, 8, 11, 12, 18, 20, 25, 26, 27, 28, 32, 36, 40, 42, 43, 44, 45, 46]:
                self.enterOuterAlt(localctx, 1)
                self.state = 358
                self.expression()
                self.state = 359
                self.match(DataLensParser.EOF)
            elif token in [-1]:
                self.enterOuterAlt(localctx, 2)
                self.state = 361
                self.match(DataLensParser.EOF)
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    def sempred(self, localctx: RuleContext, ruleIndex: int, predIndex: int):
        if self._predicates is None:
            self._predicates = dict()
        self._predicates[25] = self.exprMain_sempred
        self._predicates[26] = self.exprSecondary_sempred
        pred = self._predicates.get(ruleIndex, None)
        if pred is None:
            raise Exception("No predicate with index:" + str(ruleIndex))
        else:
            return pred(localctx, predIndex)

    def exprMain_sempred(self, localctx: ExprMainContext, predIndex: int):
        if predIndex == 0:
            return self.precpred(self._ctx, 11)

        if predIndex == 1:
            return self.precpred(self._ctx, 9)

        if predIndex == 2:
            return self.precpred(self._ctx, 8)

        if predIndex == 3:
            return self.precpred(self._ctx, 6)

        if predIndex == 4:
            return self.precpred(self._ctx, 5)

        if predIndex == 5:
            return self.precpred(self._ctx, 4)

        if predIndex == 6:
            return self.precpred(self._ctx, 7)

        if predIndex == 7:
            return self.precpred(self._ctx, 3)

    def exprSecondary_sempred(self, localctx: ExprSecondaryContext, predIndex: int):
        if predIndex == 8:
            return self.precpred(self._ctx, 3)

        if predIndex == 9:
            return self.precpred(self._ctx, 2)
