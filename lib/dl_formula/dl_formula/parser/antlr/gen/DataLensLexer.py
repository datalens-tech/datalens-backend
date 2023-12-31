import sys

from antlr4 import DFA, ATNDeserializer, Lexer, LexerATNSimulator, PredictionContextCache

if sys.version_info[1] > 5:
    from typing import TextIO
else:
    from typing.io import TextIO


def serializedATN():
    return [
        4, 0, 49, 594, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7, 4, 2, 5, 7, 5,
        2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2,
        13, 7, 13, 2, 14, 7, 14, 2, 15, 7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7,
        19, 2, 20, 7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 7, 24, 2, 25, 7, 25, 2,
        26, 7, 26, 2, 27, 7, 27, 2, 28, 7, 28, 2, 29, 7, 29, 2, 30, 7, 30, 2, 31, 7, 31, 2, 32, 7,
        32, 2, 33, 7, 33, 2, 34, 7, 34, 2, 35, 7, 35, 2, 36, 7, 36, 2, 37, 7, 37, 2, 38, 7, 38, 2,
        39, 7, 39, 2, 40, 7, 40, 2, 41, 7, 41, 2, 42, 7, 42, 2, 43, 7, 43, 2, 44, 7, 44, 2, 45, 7,
        45, 2, 46, 7, 46, 2, 47, 7, 47, 2, 48, 7, 48, 2, 49, 7, 49, 2, 50, 7, 50, 2, 51, 7, 51, 2,
        52, 7, 52, 2, 53, 7, 53, 2, 54, 7, 54, 2, 55, 7, 55, 2, 56, 7, 56, 2, 57, 7, 57, 2, 58, 7,
        58, 2, 59, 7, 59, 2, 60, 7, 60, 2, 61, 7, 61, 2, 62, 7, 62, 2, 63, 7, 63, 2, 64, 7, 64, 2,
        65, 7, 65, 2, 66, 7, 66, 2, 67, 7, 67, 2, 68, 7, 68, 2, 69, 7, 69, 2, 70, 7, 70, 2, 71, 7,
        71, 2, 72, 7, 72, 2, 73, 7, 73, 2, 74, 7, 74, 2, 75, 7, 75, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1,
        2, 1, 2, 1, 3, 1, 3, 1, 3, 1, 3, 5, 3, 165, 8, 3, 10, 3, 12, 3, 168, 9, 3, 1, 3, 3, 3, 171,
        8, 3, 1, 3, 1, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 4, 4, 180, 8, 4, 11, 4, 12, 4, 181, 1, 4, 5,
        4, 185, 8, 4, 10, 4, 12, 4, 188, 9, 4, 1, 4, 5, 4, 191, 8, 4, 10, 4, 12, 4, 194, 9, 4, 1,
        4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 5, 4, 5, 202, 8, 5, 11, 5, 12, 5, 203, 1, 5, 1, 5, 1, 6, 1, 6,
        1, 7, 1, 7, 1, 8, 1, 8, 1, 9, 1, 9, 1, 10, 1, 10, 1, 11, 1, 11, 1, 12, 1, 12, 1, 13, 1, 13,
        1, 14, 1, 14, 1, 15, 1, 15, 1, 16, 1, 16, 1, 17, 1, 17, 1, 18, 1, 18, 1, 19, 1, 19, 1, 20,
        1, 20, 1, 21, 1, 21, 1, 22, 1, 22, 1, 23, 1, 23, 1, 24, 1, 24, 1, 25, 1, 25, 1, 26, 1, 26,
        1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 28, 1, 28, 1, 28, 1, 28, 1, 29, 1, 29, 1, 29,
        1, 29, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 4, 30, 271, 8, 30, 11, 30, 12, 30,
        272, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 4, 30, 282, 8, 30, 11, 30, 12, 30,
        283, 1, 30, 1, 30, 1, 30, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 32,
        1, 32, 1, 32, 1, 32, 1, 32, 1, 33, 1, 33, 1, 33, 1, 33, 1, 33, 1, 34, 1, 34, 1, 34, 1, 34,
        1, 34, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 36, 1, 36, 1, 36, 1, 36, 1, 37,
        1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38,
        1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 40, 1, 40, 1, 40, 1, 41, 1, 41, 1, 41, 1, 41,
        1, 41, 1, 41, 1, 41, 4, 41, 353, 8, 41, 11, 41, 12, 41, 354, 1, 41, 1, 41, 1, 41, 1, 41,
        1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 42, 1, 42, 1, 42, 1, 43, 1, 43, 1, 43,
        1, 43, 1, 43, 1, 43, 1, 43, 1, 43, 1, 44, 1, 44, 1, 44, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45,
        1, 46, 1, 46, 1, 46, 1, 46, 1, 47, 1, 47, 1, 47, 1, 47, 1, 47, 1, 48, 1, 48, 1, 48, 1, 49,
        1, 49, 1, 49, 1, 49, 1, 49, 1, 49, 4, 49, 405, 8, 49, 11, 49, 12, 49, 406, 1, 49, 1, 49,
        1, 49, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 1, 52,
        1, 52, 1, 52, 1, 52, 1, 52, 1, 53, 1, 53, 1, 53, 1, 53, 1, 53, 1, 54, 1, 54, 1, 54, 1, 54,
        1, 54, 1, 54, 1, 54, 1, 55, 1, 55, 1, 56, 1, 56, 1, 57, 1, 57, 1, 58, 1, 58, 1, 59, 1, 59,
        1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 3, 59, 459, 8, 59, 1, 60,
        1, 60, 1, 61, 1, 61, 1, 62, 1, 62, 1, 63, 1, 63, 1, 64, 1, 64, 1, 64, 3, 64, 472, 8, 64,
        1, 64, 1, 64, 3, 64, 476, 8, 64, 1, 65, 1, 65, 3, 65, 480, 8, 65, 1, 65, 1, 65, 1, 66, 4,
        66, 485, 8, 66, 11, 66, 12, 66, 486, 1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 3, 67, 494, 8,
        67, 3, 67, 496, 8, 67, 1, 68, 1, 68, 1, 68, 1, 68, 5, 68, 502, 8, 68, 10, 68, 12, 68, 505,
        9, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 5, 68, 512, 8, 68, 10, 68, 12, 68, 515, 9, 68,
        1, 68, 3, 68, 518, 8, 68, 1, 69, 1, 69, 4, 69, 522, 8, 69, 11, 69, 12, 69, 523, 1, 69,
        1, 69, 1, 70, 1, 70, 3, 70, 530, 8, 70, 1, 70, 1, 70, 1, 70, 5, 70, 535, 8, 70, 10, 70,
        12, 70, 538, 9, 70, 1, 71, 1, 71, 1, 71, 1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 1, 73, 1, 73,
        1, 73, 1, 73, 1, 73, 1, 73, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74,
        1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74,
        1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74,
        1, 74, 1, 74, 3, 74, 591, 8, 74, 1, 75, 1, 75, 0, 0, 76, 1, 1, 3, 2, 5, 3, 7, 4, 9, 5, 11,
        6, 13, 0, 15, 0, 17, 0, 19, 0, 21, 0, 23, 0, 25, 0, 27, 0, 29, 0, 31, 0, 33, 0, 35, 0, 37,
        0, 39, 0, 41, 0, 43, 0, 45, 0, 47, 0, 49, 0, 51, 0, 53, 0, 55, 7, 57, 8, 59, 9, 61, 10, 63,
        11, 65, 12, 67, 13, 69, 14, 71, 15, 73, 16, 75, 17, 77, 18, 79, 19, 81, 20, 83, 21, 85,
        22, 87, 23, 89, 24, 91, 25, 93, 26, 95, 27, 97, 28, 99, 29, 101, 30, 103, 31, 105, 32,
        107, 33, 109, 34, 111, 35, 113, 36, 115, 37, 117, 38, 119, 39, 121, 40, 123, 41, 125,
        0, 127, 0, 129, 0, 131, 0, 133, 42, 135, 43, 137, 44, 139, 45, 141, 46, 143, 0, 145,
        0, 147, 47, 149, 48, 151, 49, 1, 0, 34, 1, 0, 10, 10, 1, 1, 10, 10, 1, 0, 42, 42, 2, 0,
        42, 42, 47, 47, 4, 0, 9, 10, 12, 13, 32, 32, 160, 160, 2, 0, 65, 65, 97, 97, 2, 0, 66,
        66, 98, 98, 2, 0, 67, 67, 99, 99, 2, 0, 68, 68, 100, 100, 2, 0, 69, 69, 101, 101, 2, 0,
        70, 70, 102, 102, 2, 0, 71, 71, 103, 103, 2, 0, 72, 72, 104, 104, 2, 0, 73, 73, 105,
        105, 2, 0, 75, 75, 107, 107, 2, 0, 76, 76, 108, 108, 2, 0, 77, 77, 109, 109, 2, 0, 78,
        78, 110, 110, 2, 0, 79, 79, 111, 111, 2, 0, 82, 82, 114, 114, 2, 0, 83, 83, 115, 115,
        2, 0, 84, 84, 116, 116, 2, 0, 85, 85, 117, 117, 2, 0, 87, 87, 119, 119, 2, 0, 88, 88,
        120, 120, 2, 0, 89, 89, 121, 121, 3, 0, 37, 37, 42, 42, 47, 47, 1, 0, 48, 57, 2, 0, 65,
        90, 97, 122, 2, 0, 43, 43, 45, 45, 1, 0, 39, 39, 1, 0, 34, 34, 4, 0, 91, 91, 93, 93, 123,
        123, 125, 125, 3, 0, 32, 32, 84, 84, 116, 116, 601, 0, 1, 1, 0, 0, 0, 0, 3, 1, 0, 0, 0,
        0, 5, 1, 0, 0, 0, 0, 7, 1, 0, 0, 0, 0, 9, 1, 0, 0, 0, 0, 11, 1, 0, 0, 0, 0, 55, 1, 0, 0, 0, 0,
        57, 1, 0, 0, 0, 0, 59, 1, 0, 0, 0, 0, 61, 1, 0, 0, 0, 0, 63, 1, 0, 0, 0, 0, 65, 1, 0, 0, 0, 0,
        67, 1, 0, 0, 0, 0, 69, 1, 0, 0, 0, 0, 71, 1, 0, 0, 0, 0, 73, 1, 0, 0, 0, 0, 75, 1, 0, 0, 0, 0,
        77, 1, 0, 0, 0, 0, 79, 1, 0, 0, 0, 0, 81, 1, 0, 0, 0, 0, 83, 1, 0, 0, 0, 0, 85, 1, 0, 0, 0, 0,
        87, 1, 0, 0, 0, 0, 89, 1, 0, 0, 0, 0, 91, 1, 0, 0, 0, 0, 93, 1, 0, 0, 0, 0, 95, 1, 0, 0, 0, 0,
        97, 1, 0, 0, 0, 0, 99, 1, 0, 0, 0, 0, 101, 1, 0, 0, 0, 0, 103, 1, 0, 0, 0, 0, 105, 1, 0, 0,
        0, 0, 107, 1, 0, 0, 0, 0, 109, 1, 0, 0, 0, 0, 111, 1, 0, 0, 0, 0, 113, 1, 0, 0, 0, 0, 115,
        1, 0, 0, 0, 0, 117, 1, 0, 0, 0, 0, 119, 1, 0, 0, 0, 0, 121, 1, 0, 0, 0, 0, 123, 1, 0, 0, 0,
        0, 133, 1, 0, 0, 0, 0, 135, 1, 0, 0, 0, 0, 137, 1, 0, 0, 0, 0, 139, 1, 0, 0, 0, 0, 141, 1,
        0, 0, 0, 0, 147, 1, 0, 0, 0, 0, 149, 1, 0, 0, 0, 0, 151, 1, 0, 0, 0, 1, 153, 1, 0, 0, 0, 3,
        155, 1, 0, 0, 0, 5, 158, 1, 0, 0, 0, 7, 160, 1, 0, 0, 0, 9, 174, 1, 0, 0, 0, 11, 201, 1, 0,
        0, 0, 13, 207, 1, 0, 0, 0, 15, 209, 1, 0, 0, 0, 17, 211, 1, 0, 0, 0, 19, 213, 1, 0, 0, 0,
        21, 215, 1, 0, 0, 0, 23, 217, 1, 0, 0, 0, 25, 219, 1, 0, 0, 0, 27, 221, 1, 0, 0, 0, 29, 223,
        1, 0, 0, 0, 31, 225, 1, 0, 0, 0, 33, 227, 1, 0, 0, 0, 35, 229, 1, 0, 0, 0, 37, 231, 1, 0,
        0, 0, 39, 233, 1, 0, 0, 0, 41, 235, 1, 0, 0, 0, 43, 237, 1, 0, 0, 0, 45, 239, 1, 0, 0, 0,
        47, 241, 1, 0, 0, 0, 49, 243, 1, 0, 0, 0, 51, 245, 1, 0, 0, 0, 53, 247, 1, 0, 0, 0, 55, 249,
        1, 0, 0, 0, 57, 255, 1, 0, 0, 0, 59, 259, 1, 0, 0, 0, 61, 263, 1, 0, 0, 0, 63, 288, 1, 0,
        0, 0, 65, 296, 1, 0, 0, 0, 67, 301, 1, 0, 0, 0, 69, 306, 1, 0, 0, 0, 71, 311, 1, 0, 0, 0,
        73, 318, 1, 0, 0, 0, 75, 322, 1, 0, 0, 0, 77, 330, 1, 0, 0, 0, 79, 336, 1, 0, 0, 0, 81, 342,
        1, 0, 0, 0, 83, 345, 1, 0, 0, 0, 85, 367, 1, 0, 0, 0, 87, 370, 1, 0, 0, 0, 89, 378, 1, 0,
        0, 0, 91, 381, 1, 0, 0, 0, 93, 386, 1, 0, 0, 0, 95, 390, 1, 0, 0, 0, 97, 395, 1, 0, 0, 0,
        99, 398, 1, 0, 0, 0, 101, 411, 1, 0, 0, 0, 103, 416, 1, 0, 0, 0, 105, 422, 1, 0, 0, 0, 107,
        427, 1, 0, 0, 0, 109, 432, 1, 0, 0, 0, 111, 439, 1, 0, 0, 0, 113, 441, 1, 0, 0, 0, 115,
        443, 1, 0, 0, 0, 117, 445, 1, 0, 0, 0, 119, 458, 1, 0, 0, 0, 121, 460, 1, 0, 0, 0, 123,
        462, 1, 0, 0, 0, 125, 464, 1, 0, 0, 0, 127, 466, 1, 0, 0, 0, 129, 475, 1, 0, 0, 0, 131,
        477, 1, 0, 0, 0, 133, 484, 1, 0, 0, 0, 135, 495, 1, 0, 0, 0, 137, 517, 1, 0, 0, 0, 139,
        519, 1, 0, 0, 0, 141, 529, 1, 0, 0, 0, 143, 539, 1, 0, 0, 0, 145, 542, 1, 0, 0, 0, 147,
        547, 1, 0, 0, 0, 149, 590, 1, 0, 0, 0, 151, 592, 1, 0, 0, 0, 153, 154, 5, 35, 0, 0, 154,
        2, 1, 0, 0, 0, 155, 156, 5, 35, 0, 0, 156, 157, 5, 35, 0, 0, 157, 4, 1, 0, 0, 0, 158, 159,
        5, 44, 0, 0, 159, 6, 1, 0, 0, 0, 160, 161, 5, 45, 0, 0, 161, 162, 5, 45, 0, 0, 162, 166,
        1, 0, 0, 0, 163, 165, 8, 0, 0, 0, 164, 163, 1, 0, 0, 0, 165, 168, 1, 0, 0, 0, 166, 164,
        1, 0, 0, 0, 166, 167, 1, 0, 0, 0, 167, 170, 1, 0, 0, 0, 168, 166, 1, 0, 0, 0, 169, 171,
        7, 1, 0, 0, 170, 169, 1, 0, 0, 0, 171, 172, 1, 0, 0, 0, 172, 173, 6, 3, 0, 0, 173, 8, 1,
        0, 0, 0, 174, 175, 5, 47, 0, 0, 175, 176, 5, 42, 0, 0, 176, 186, 1, 0, 0, 0, 177, 185,
        8, 2, 0, 0, 178, 180, 7, 2, 0, 0, 179, 178, 1, 0, 0, 0, 180, 181, 1, 0, 0, 0, 181, 179,
        1, 0, 0, 0, 181, 182, 1, 0, 0, 0, 182, 183, 1, 0, 0, 0, 183, 185, 8, 3, 0, 0, 184, 177,
        1, 0, 0, 0, 184, 179, 1, 0, 0, 0, 185, 188, 1, 0, 0, 0, 186, 184, 1, 0, 0, 0, 186, 187,
        1, 0, 0, 0, 187, 192, 1, 0, 0, 0, 188, 186, 1, 0, 0, 0, 189, 191, 7, 2, 0, 0, 190, 189,
        1, 0, 0, 0, 191, 194, 1, 0, 0, 0, 192, 190, 1, 0, 0, 0, 192, 193, 1, 0, 0, 0, 193, 195,
        1, 0, 0, 0, 194, 192, 1, 0, 0, 0, 195, 196, 5, 42, 0, 0, 196, 197, 5, 47, 0, 0, 197, 198,
        1, 0, 0, 0, 198, 199, 6, 4, 0, 0, 199, 10, 1, 0, 0, 0, 200, 202, 7, 4, 0, 0, 201, 200, 1,
        0, 0, 0, 202, 203, 1, 0, 0, 0, 203, 201, 1, 0, 0, 0, 203, 204, 1, 0, 0, 0, 204, 205, 1,
        0, 0, 0, 205, 206, 6, 5, 0, 0, 206, 12, 1, 0, 0, 0, 207, 208, 7, 5, 0, 0, 208, 14, 1, 0,
        0, 0, 209, 210, 7, 6, 0, 0, 210, 16, 1, 0, 0, 0, 211, 212, 7, 7, 0, 0, 212, 18, 1, 0, 0,
        0, 213, 214, 7, 8, 0, 0, 214, 20, 1, 0, 0, 0, 215, 216, 7, 9, 0, 0, 216, 22, 1, 0, 0, 0,
        217, 218, 7, 10, 0, 0, 218, 24, 1, 0, 0, 0, 219, 220, 7, 11, 0, 0, 220, 26, 1, 0, 0, 0,
        221, 222, 7, 12, 0, 0, 222, 28, 1, 0, 0, 0, 223, 224, 7, 13, 0, 0, 224, 30, 1, 0, 0, 0,
        225, 226, 7, 14, 0, 0, 226, 32, 1, 0, 0, 0, 227, 228, 7, 15, 0, 0, 228, 34, 1, 0, 0, 0,
        229, 230, 7, 16, 0, 0, 230, 36, 1, 0, 0, 0, 231, 232, 7, 17, 0, 0, 232, 38, 1, 0, 0, 0,
        233, 234, 7, 18, 0, 0, 234, 40, 1, 0, 0, 0, 235, 236, 7, 19, 0, 0, 236, 42, 1, 0, 0, 0,
        237, 238, 7, 20, 0, 0, 238, 44, 1, 0, 0, 0, 239, 240, 7, 21, 0, 0, 240, 46, 1, 0, 0, 0,
        241, 242, 7, 22, 0, 0, 242, 48, 1, 0, 0, 0, 243, 244, 7, 23, 0, 0, 244, 50, 1, 0, 0, 0,
        245, 246, 7, 24, 0, 0, 246, 52, 1, 0, 0, 0, 247, 248, 7, 25, 0, 0, 248, 54, 1, 0, 0, 0,
        249, 250, 3, 13, 6, 0, 250, 251, 3, 35, 17, 0, 251, 252, 3, 39, 19, 0, 252, 253, 3, 37,
        18, 0, 253, 254, 3, 25, 12, 0, 254, 56, 1, 0, 0, 0, 255, 256, 3, 13, 6, 0, 256, 257, 3,
        37, 18, 0, 257, 258, 3, 19, 9, 0, 258, 58, 1, 0, 0, 0, 259, 260, 3, 13, 6, 0, 260, 261,
        3, 43, 21, 0, 261, 262, 3, 17, 8, 0, 262, 60, 1, 0, 0, 0, 263, 264, 3, 15, 7, 0, 264, 265,
        3, 21, 10, 0, 265, 266, 3, 23, 11, 0, 266, 267, 3, 39, 19, 0, 267, 268, 3, 41, 20, 0,
        268, 270, 3, 21, 10, 0, 269, 271, 5, 32, 0, 0, 270, 269, 1, 0, 0, 0, 271, 272, 1, 0, 0,
        0, 272, 270, 1, 0, 0, 0, 272, 273, 1, 0, 0, 0, 273, 274, 1, 0, 0, 0, 274, 275, 3, 23, 11,
        0, 275, 276, 3, 29, 14, 0, 276, 277, 3, 33, 16, 0, 277, 278, 3, 45, 22, 0, 278, 279,
        3, 21, 10, 0, 279, 281, 3, 41, 20, 0, 280, 282, 5, 32, 0, 0, 281, 280, 1, 0, 0, 0, 282,
        283, 1, 0, 0, 0, 283, 281, 1, 0, 0, 0, 283, 284, 1, 0, 0, 0, 284, 285, 1, 0, 0, 0, 285,
        286, 3, 15, 7, 0, 286, 287, 3, 53, 26, 0, 287, 62, 1, 0, 0, 0, 288, 289, 3, 15, 7, 0, 289,
        290, 3, 21, 10, 0, 290, 291, 3, 45, 22, 0, 291, 292, 3, 49, 24, 0, 292, 293, 3, 21, 10,
        0, 293, 294, 3, 21, 10, 0, 294, 295, 3, 37, 18, 0, 295, 64, 1, 0, 0, 0, 296, 297, 3, 17,
        8, 0, 297, 298, 3, 13, 6, 0, 298, 299, 3, 43, 21, 0, 299, 300, 3, 21, 10, 0, 300, 66,
        1, 0, 0, 0, 301, 302, 3, 19, 9, 0, 302, 303, 3, 21, 10, 0, 303, 304, 3, 43, 21, 0, 304,
        305, 3, 17, 8, 0, 305, 68, 1, 0, 0, 0, 306, 307, 3, 21, 10, 0, 307, 308, 3, 33, 16, 0,
        308, 309, 3, 43, 21, 0, 309, 310, 3, 21, 10, 0, 310, 70, 1, 0, 0, 0, 311, 312, 3, 21,
        10, 0, 312, 313, 3, 33, 16, 0, 313, 314, 3, 43, 21, 0, 314, 315, 3, 21, 10, 0, 315, 316,
        3, 29, 14, 0, 316, 317, 3, 23, 11, 0, 317, 72, 1, 0, 0, 0, 318, 319, 3, 21, 10, 0, 319,
        320, 3, 37, 18, 0, 320, 321, 3, 19, 9, 0, 321, 74, 1, 0, 0, 0, 322, 323, 3, 21, 10, 0,
        323, 324, 3, 51, 25, 0, 324, 325, 3, 17, 8, 0, 325, 326, 3, 33, 16, 0, 326, 327, 3, 47,
        23, 0, 327, 328, 3, 19, 9, 0, 328, 329, 3, 21, 10, 0, 329, 76, 1, 0, 0, 0, 330, 331, 3,
        23, 11, 0, 331, 332, 3, 13, 6, 0, 332, 333, 3, 33, 16, 0, 333, 334, 3, 43, 21, 0, 334,
        335, 3, 21, 10, 0, 335, 78, 1, 0, 0, 0, 336, 337, 3, 23, 11, 0, 337, 338, 3, 29, 14, 0,
        338, 339, 3, 51, 25, 0, 339, 340, 3, 21, 10, 0, 340, 341, 3, 19, 9, 0, 341, 80, 1, 0,
        0, 0, 342, 343, 3, 29, 14, 0, 343, 344, 3, 23, 11, 0, 344, 82, 1, 0, 0, 0, 345, 346, 3,
        29, 14, 0, 346, 347, 3, 25, 12, 0, 347, 348, 3, 37, 18, 0, 348, 349, 3, 39, 19, 0, 349,
        350, 3, 41, 20, 0, 350, 352, 3, 21, 10, 0, 351, 353, 5, 32, 0, 0, 352, 351, 1, 0, 0, 0,
        353, 354, 1, 0, 0, 0, 354, 352, 1, 0, 0, 0, 354, 355, 1, 0, 0, 0, 355, 356, 1, 0, 0, 0,
        356, 357, 3, 19, 9, 0, 357, 358, 3, 29, 14, 0, 358, 359, 3, 35, 17, 0, 359, 360, 3, 21,
        10, 0, 360, 361, 3, 37, 18, 0, 361, 362, 3, 43, 21, 0, 362, 363, 3, 29, 14, 0, 363, 364,
        3, 39, 19, 0, 364, 365, 3, 37, 18, 0, 365, 366, 3, 43, 21, 0, 366, 84, 1, 0, 0, 0, 367,
        368, 3, 29, 14, 0, 368, 369, 3, 37, 18, 0, 369, 86, 1, 0, 0, 0, 370, 371, 3, 29, 14, 0,
        371, 372, 3, 37, 18, 0, 372, 373, 3, 17, 8, 0, 373, 374, 3, 33, 16, 0, 374, 375, 3, 47,
        23, 0, 375, 376, 3, 19, 9, 0, 376, 377, 3, 21, 10, 0, 377, 88, 1, 0, 0, 0, 378, 379, 3,
        29, 14, 0, 379, 380, 3, 43, 21, 0, 380, 90, 1, 0, 0, 0, 381, 382, 3, 33, 16, 0, 382, 383,
        3, 29, 14, 0, 383, 384, 3, 31, 15, 0, 384, 385, 3, 21, 10, 0, 385, 92, 1, 0, 0, 0, 386,
        387, 3, 37, 18, 0, 387, 388, 3, 39, 19, 0, 388, 389, 3, 45, 22, 0, 389, 94, 1, 0, 0, 0,
        390, 391, 3, 37, 18, 0, 391, 392, 3, 47, 23, 0, 392, 393, 3, 33, 16, 0, 393, 394, 3,
        33, 16, 0, 394, 96, 1, 0, 0, 0, 395, 396, 3, 39, 19, 0, 396, 397, 3, 41, 20, 0, 397, 98,
        1, 0, 0, 0, 398, 399, 3, 39, 19, 0, 399, 400, 3, 41, 20, 0, 400, 401, 3, 19, 9, 0, 401,
        402, 3, 21, 10, 0, 402, 404, 3, 41, 20, 0, 403, 405, 5, 32, 0, 0, 404, 403, 1, 0, 0, 0,
        405, 406, 1, 0, 0, 0, 406, 404, 1, 0, 0, 0, 406, 407, 1, 0, 0, 0, 407, 408, 1, 0, 0, 0,
        408, 409, 3, 15, 7, 0, 409, 410, 3, 53, 26, 0, 410, 100, 1, 0, 0, 0, 411, 412, 3, 45,
        22, 0, 412, 413, 3, 27, 13, 0, 413, 414, 3, 21, 10, 0, 414, 415, 3, 37, 18, 0, 415, 102,
        1, 0, 0, 0, 416, 417, 3, 45, 22, 0, 417, 418, 3, 39, 19, 0, 418, 419, 3, 45, 22, 0, 419,
        420, 3, 13, 6, 0, 420, 421, 3, 33, 16, 0, 421, 104, 1, 0, 0, 0, 422, 423, 3, 45, 22, 0,
        423, 424, 3, 41, 20, 0, 424, 425, 3, 47, 23, 0, 425, 426, 3, 21, 10, 0, 426, 106, 1,
        0, 0, 0, 427, 428, 3, 49, 24, 0, 428, 429, 3, 27, 13, 0, 429, 430, 3, 21, 10, 0, 430,
        431, 3, 37, 18, 0, 431, 108, 1, 0, 0, 0, 432, 433, 3, 49, 24, 0, 433, 434, 3, 29, 14,
        0, 434, 435, 3, 45, 22, 0, 435, 436, 3, 27, 13, 0, 436, 437, 3, 29, 14, 0, 437, 438,
        3, 37, 18, 0, 438, 110, 1, 0, 0, 0, 439, 440, 5, 43, 0, 0, 440, 112, 1, 0, 0, 0, 441, 442,
        5, 45, 0, 0, 442, 114, 1, 0, 0, 0, 443, 444, 5, 94, 0, 0, 444, 116, 1, 0, 0, 0, 445, 446,
        7, 26, 0, 0, 446, 118, 1, 0, 0, 0, 447, 459, 5, 61, 0, 0, 448, 449, 5, 33, 0, 0, 449, 459,
        5, 61, 0, 0, 450, 451, 5, 60, 0, 0, 451, 459, 5, 62, 0, 0, 452, 459, 5, 62, 0, 0, 453,
        454, 5, 62, 0, 0, 454, 459, 5, 61, 0, 0, 455, 459, 5, 60, 0, 0, 456, 457, 5, 60, 0, 0,
        457, 459, 5, 61, 0, 0, 458, 447, 1, 0, 0, 0, 458, 448, 1, 0, 0, 0, 458, 450, 1, 0, 0, 0,
        458, 452, 1, 0, 0, 0, 458, 453, 1, 0, 0, 0, 458, 455, 1, 0, 0, 0, 458, 456, 1, 0, 0, 0,
        459, 120, 1, 0, 0, 0, 460, 461, 5, 40, 0, 0, 461, 122, 1, 0, 0, 0, 462, 463, 5, 41, 0,
        0, 463, 124, 1, 0, 0, 0, 464, 465, 7, 27, 0, 0, 465, 126, 1, 0, 0, 0, 466, 467, 7, 28,
        0, 0, 467, 128, 1, 0, 0, 0, 468, 469, 3, 133, 66, 0, 469, 471, 5, 46, 0, 0, 470, 472,
        3, 133, 66, 0, 471, 470, 1, 0, 0, 0, 471, 472, 1, 0, 0, 0, 472, 476, 1, 0, 0, 0, 473, 474,
        5, 46, 0, 0, 474, 476, 3, 133, 66, 0, 475, 468, 1, 0, 0, 0, 475, 473, 1, 0, 0, 0, 476,
        130, 1, 0, 0, 0, 477, 479, 7, 9, 0, 0, 478, 480, 7, 29, 0, 0, 479, 478, 1, 0, 0, 0, 479,
        480, 1, 0, 0, 0, 480, 481, 1, 0, 0, 0, 481, 482, 3, 133, 66, 0, 482, 132, 1, 0, 0, 0, 483,
        485, 3, 125, 62, 0, 484, 483, 1, 0, 0, 0, 485, 486, 1, 0, 0, 0, 486, 484, 1, 0, 0, 0, 486,
        487, 1, 0, 0, 0, 487, 134, 1, 0, 0, 0, 488, 489, 3, 133, 66, 0, 489, 490, 3, 131, 65,
        0, 490, 496, 1, 0, 0, 0, 491, 493, 3, 129, 64, 0, 492, 494, 3, 131, 65, 0, 493, 492,
        1, 0, 0, 0, 493, 494, 1, 0, 0, 0, 494, 496, 1, 0, 0, 0, 495, 488, 1, 0, 0, 0, 495, 491,
        1, 0, 0, 0, 496, 136, 1, 0, 0, 0, 497, 503, 5, 39, 0, 0, 498, 499, 5, 92, 0, 0, 499, 502,
        5, 39, 0, 0, 500, 502, 8, 30, 0, 0, 501, 498, 1, 0, 0, 0, 501, 500, 1, 0, 0, 0, 502, 505,
        1, 0, 0, 0, 503, 501, 1, 0, 0, 0, 503, 504, 1, 0, 0, 0, 504, 506, 1, 0, 0, 0, 505, 503,
        1, 0, 0, 0, 506, 518, 5, 39, 0, 0, 507, 513, 5, 34, 0, 0, 508, 509, 5, 92, 0, 0, 509, 512,
        5, 34, 0, 0, 510, 512, 8, 31, 0, 0, 511, 508, 1, 0, 0, 0, 511, 510, 1, 0, 0, 0, 512, 515,
        1, 0, 0, 0, 513, 511, 1, 0, 0, 0, 513, 514, 1, 0, 0, 0, 514, 516, 1, 0, 0, 0, 515, 513,
        1, 0, 0, 0, 516, 518, 5, 34, 0, 0, 517, 497, 1, 0, 0, 0, 517, 507, 1, 0, 0, 0, 518, 138,
        1, 0, 0, 0, 519, 521, 5, 91, 0, 0, 520, 522, 8, 32, 0, 0, 521, 520, 1, 0, 0, 0, 522, 523,
        1, 0, 0, 0, 523, 521, 1, 0, 0, 0, 523, 524, 1, 0, 0, 0, 524, 525, 1, 0, 0, 0, 525, 526,
        5, 93, 0, 0, 526, 140, 1, 0, 0, 0, 527, 530, 3, 127, 63, 0, 528, 530, 5, 95, 0, 0, 529,
        527, 1, 0, 0, 0, 529, 528, 1, 0, 0, 0, 530, 536, 1, 0, 0, 0, 531, 535, 3, 127, 63, 0, 532,
        535, 3, 125, 62, 0, 533, 535, 5, 95, 0, 0, 534, 531, 1, 0, 0, 0, 534, 532, 1, 0, 0, 0,
        534, 533, 1, 0, 0, 0, 535, 538, 1, 0, 0, 0, 536, 534, 1, 0, 0, 0, 536, 537, 1, 0, 0, 0,
        537, 142, 1, 0, 0, 0, 538, 536, 1, 0, 0, 0, 539, 540, 7, 27, 0, 0, 540, 541, 7, 27, 0,
        0, 541, 144, 1, 0, 0, 0, 542, 543, 7, 27, 0, 0, 543, 544, 7, 27, 0, 0, 544, 545, 7, 27,
        0, 0, 545, 546, 7, 27, 0, 0, 546, 146, 1, 0, 0, 0, 547, 548, 3, 145, 72, 0, 548, 549,
        5, 45, 0, 0, 549, 550, 3, 143, 71, 0, 550, 551, 5, 45, 0, 0, 551, 552, 3, 143, 71, 0,
        552, 148, 1, 0, 0, 0, 553, 554, 3, 145, 72, 0, 554, 555, 5, 45, 0, 0, 555, 556, 3, 143,
        71, 0, 556, 557, 5, 45, 0, 0, 557, 558, 3, 143, 71, 0, 558, 559, 7, 33, 0, 0, 559, 560,
        3, 143, 71, 0, 560, 561, 5, 58, 0, 0, 561, 562, 3, 143, 71, 0, 562, 563, 5, 58, 0, 0,
        563, 564, 3, 143, 71, 0, 564, 591, 1, 0, 0, 0, 565, 566, 3, 145, 72, 0, 566, 567, 5,
        45, 0, 0, 567, 568, 3, 143, 71, 0, 568, 569, 5, 45, 0, 0, 569, 570, 3, 143, 71, 0, 570,
        571, 7, 33, 0, 0, 571, 572, 3, 143, 71, 0, 572, 573, 5, 58, 0, 0, 573, 574, 3, 143, 71,
        0, 574, 591, 1, 0, 0, 0, 575, 576, 3, 145, 72, 0, 576, 577, 5, 45, 0, 0, 577, 578, 3,
        143, 71, 0, 578, 579, 5, 45, 0, 0, 579, 580, 3, 143, 71, 0, 580, 581, 7, 33, 0, 0, 581,
        582, 3, 143, 71, 0, 582, 591, 1, 0, 0, 0, 583, 584, 3, 145, 72, 0, 584, 585, 5, 45, 0,
        0, 585, 586, 3, 143, 71, 0, 586, 587, 5, 45, 0, 0, 587, 588, 3, 143, 71, 0, 588, 589,
        7, 21, 0, 0, 589, 591, 1, 0, 0, 0, 590, 553, 1, 0, 0, 0, 590, 565, 1, 0, 0, 0, 590, 575,
        1, 0, 0, 0, 590, 583, 1, 0, 0, 0, 591, 150, 1, 0, 0, 0, 592, 593, 9, 0, 0, 0, 593, 152,
        1, 0, 0, 0, 29, 0, 166, 170, 181, 184, 186, 192, 203, 272, 283, 354, 406, 458, 471,
        475, 479, 486, 493, 495, 501, 503, 511, 513, 517, 523, 529, 534, 536, 590, 1, 6,
        0, 0
    ]


class DataLensLexer(Lexer):

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [DFA(ds, i) for i, ds in enumerate(atn.decisionToState)]

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

    channelNames = [u"DEFAULT_TOKEN_CHANNEL", u"HIDDEN"]

    modeNames = ["DEFAULT_MODE"]

    literalNames = ["<INVALID>",
                    "'#'", "'##'", "','", "'+'", "'-'", "'^'", "'('", "')'"]

    symbolicNames = ["<INVALID>",
                     "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT", "WS", "AMONG",
                     "AND", "ASC", "BEFORE_FILTER_BY", "BETWEEN", "CASE", "DESC",
                     "ELSE", "ELSEIF", "END", "EXCLUDE", "FALSE", "FIXED", "IF",
                     "IGNORE_DIMENSIONS", "IN", "INCLUDE", "IS", "LIKE", "NOT", "NULL",
                     "OR", "ORDER_BY", "THEN", "TOTAL", "TRUE", "WHEN", "WITHIN",
                     "PLUS", "MINUS", "POWER", "MULDIV", "COMPARISON", "OPENING_PAR",
                     "CLOSING_PAR", "INT", "FLOAT", "ESCAPED_STRING", "FIELD_NAME",
                     "FUNC_NAME", "DATE_INNER", "DATETIME_INNER", "UNEXPECTED_CHARACTER"]

    ruleNames = ["T__0", "T__1", "T__2", "SINGLE_LINE_COMMENT", "MULTI_LINE_COMMENT",
                 "WS", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K",
                 "L", "M", "N", "O", "R", "S", "T", "U", "W", "X", "Y",
                 "AMONG", "AND", "ASC", "BEFORE_FILTER_BY", "BETWEEN",
                 "CASE", "DESC", "ELSE", "ELSEIF", "END", "EXCLUDE", "FALSE",
                 "FIXED", "IF", "IGNORE_DIMENSIONS", "IN", "INCLUDE", "IS",
                 "LIKE", "NOT", "NULL", "OR", "ORDER_BY", "THEN", "TOTAL",
                 "TRUE", "WHEN", "WITHIN", "PLUS", "MINUS", "POWER", "MULDIV",
                 "COMPARISON", "OPENING_PAR", "CLOSING_PAR", "DIGIT", "LETTER",
                 "DECIMAL", "EXP", "INT", "FLOAT", "ESCAPED_STRING", "FIELD_NAME",
                 "FUNC_NAME", "DD", "DDDD", "DATE_INNER", "DATETIME_INNER",
                 "UNEXPECTED_CHARACTER"]

    grammarFileName = "DataLens.g4"

    def __init__(self, input=None, output: TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.11.1")
        self._interp = LexerATNSimulator(self, self.atn, self.decisionsToDFA, PredictionContextCache())
        self._actions = None
        self._predicates = None
