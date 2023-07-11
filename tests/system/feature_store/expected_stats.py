# Copyright 2023 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import math

expected_stats = {
    "count": {
        "bad": 190.0,
        "department": 190.0,
        "hr": 190.0,
        "hr_is_error": 190.0,
        "is_in_bed": 190.0,
        "is_in_bed_is_error": 190.0,
        "movements": 190.0,
        "movements_is_error": 190.0,
        "patient_id": 190.0,
        "room": 190.0,
        "rr": 190.0,
        "rr_is_error": 190.0,
        "spo2": 190.0,
        "spo2_is_error": 190.0,
        "timestamp": 190.0,
        "turn_count": 190.0,
        "turn_count_is_error": 190.0,
    },
    "mean": {
        "bad": 49.10526315789474,
        "department": math.nan,
        "hr": 220.0,
        "hr_is_error": 0.015789473684210527,
        "is_in_bed": 1.0,
        "is_in_bed_is_error": 0.010526315789473684,
        "movements": 3.6203568596815643,
        "movements_is_error": 0.015789473684210527,
        "patient_id": math.nan,
        "room": 1.4947368421052631,
        "rr": 24.68421052631579,
        "rr_is_error": 0.015789473684210527,
        "spo2": 98.77894736842106,
        "spo2_is_error": 0.015789473684210527,
        "timestamp": "2020-12-01T17:28:31.695824+00:00",
        "turn_count": 1.3398340922970073,
        "turn_count_is_error": 0.015789473684210527,
    },
    "std": {
        "bad": 30.111424007351214,
        "department": math.nan,
        "hr": 0.0,
        "hr_is_error": 0.12498955679015682,
        "is_in_bed": 0.0,
        "is_in_bed_is_error": 0.10232605238616795,
        "movements": 3.2840955409124373,
        "movements_is_error": 0.12498955679015682,
        "patient_id": math.nan,
        "room": 0.5012932314788251,
        "rr": 2.499791135803136,
        "rr_is_error": 0.12498955679015682,
        "spo2": 1.749853795062193,
        "spo2_is_error": 0.12498955679015682,
        "timestamp": 165.43403820590876,
        "turn_count": 1.2704837468566184,
        "turn_count_is_error": 0.12498955679015682,
    },
    "min": {
        "bad": 4.0,
        "department": "01e9fe31-76de-45f0-9aed-0f94cc97bca0",
        "hr": 220.0,
        "hr_is_error": False,
        "is_in_bed": 1.0,
        "is_in_bed_is_error": False,
        "movements": 0.0,
        "movements_is_error": False,
        "patient_id": "025-79-2727",
        "room": 1.0,
        "rr": 5.0,
        "rr_is_error": False,
        "spo2": 85.0,
        "spo2_is_error": False,
        "timestamp": "2020-12-01T17:24:15.906352+00:00",
        "turn_count": 0.0,
        "turn_count_is_error": False,
    },
    "25%": {
        "bad": 17.0,
        "department": math.nan,
        "hr": 220.0,
        "hr_is_error": False,
        "is_in_bed": 1.0,
        "is_in_bed_is_error": False,
        "movements": 0.18989211159466102,
        "movements_is_error": False,
        "patient_id": math.nan,
        "room": 1.0,
        "rr": 25.0,
        "rr_is_error": False,
        "spo2": 99.0,
        "spo2_is_error": False,
        "timestamp": "2020-12-01T17:26:15.906352+00:00",
        "turn_count": 0.0,
        "turn_count_is_error": False,
    },
    "50%": {
        "bad": 50.0,
        "department": math.nan,
        "hr": 220.0,
        "hr_is_error": False,
        "is_in_bed": 1.0,
        "is_in_bed_is_error": False,
        "movements": 2.9413387174437027,
        "movements_is_error": False,
        "patient_id": math.nan,
        "room": 1.0,
        "rr": 25.0,
        "rr_is_error": False,
        "spo2": 99.0,
        "spo2_is_error": False,
        "timestamp": "2020-12-01T17:28:15.906352+00:00",
        "turn_count": 1.1724099011618052,
        "turn_count_is_error": False,
    },
    "75%": {
        "bad": 76.0,
        "department": math.nan,
        "hr": 220.0,
        "hr_is_error": False,
        "is_in_bed": 1.0,
        "is_in_bed_is_error": False,
        "movements": 6.010198369762724,
        "movements_is_error": False,
        "patient_id": math.nan,
        "room": 2.0,
        "rr": 25.0,
        "rr_is_error": False,
        "spo2": 99.0,
        "spo2_is_error": False,
        "timestamp": "2020-12-01T17:31:15.906352+00:00",
        "turn_count": 2.951729964062169,
        "turn_count_is_error": False,
    },
    "max": {
        "bad": 95.0,
        "department": "4685f09b-51cc-48c9-b8e5-32e3175d4759",
        "hr": 220.0,
        "hr_is_error": True,
        "is_in_bed": 1.0,
        "is_in_bed_is_error": True,
        "movements": 10.0,
        "movements_is_error": True,
        "patient_id": "838-21-8151",
        "room": 2.0,
        "rr": 25.0,
        "rr_is_error": True,
        "spo2": 99.0,
        "spo2_is_error": True,
        "timestamp": "2020-12-01T17:33:15.906352+00:00",
        "turn_count": 3.0,
        "turn_count_is_error": True,
    },
    "hist": {
        "bad": [
            [19, 9, 28, 0, 9, 0, 0, 10, 0, 10, 20, 0, 9, 0, 19, 19, 10, 0, 18, 10],
            [
                4.0,
                8.55,
                13.1,
                17.65,
                22.2,
                26.75,
                31.3,
                35.85,
                40.4,
                44.95,
                49.5,
                54.05,
                58.6,
                63.15,
                67.7,
                72.25,
                76.8,
                81.35,
                85.9,
                90.45,
            ],
        ],
        "department": math.nan,
        "hr": [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 190],
            [
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
                220.0,
            ],
        ],
        "hr_is_error": [
            [187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
            [
                0.0,
                0.05,
                0.1,
                0.15,
                0.2,
                0.25,
                0.3,
                0.35,
                0.4,
                0.45,
                0.5,
                0.55,
                0.6,
                0.65,
                0.7,
                0.75,
                0.8,
                0.85,
                0.9,
                0.95,
            ],
        ],
        "is_in_bed": [
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 190],
            [
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ],
        ],
        "is_in_bed_is_error": [
            [188, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
            [
                0.0,
                0.05,
                0.1,
                0.15,
                0.2,
                0.25,
                0.3,
                0.35,
                0.4,
                0.45,
                0.5,
                0.55,
                0.6,
                0.65,
                0.7,
                0.75,
                0.8,
                0.85,
                0.9,
                0.95,
            ],
        ],
        "movements": [
            [52, 4, 8, 13, 13, 5, 10, 6, 5, 11, 11, 4, 9, 3, 3, 5, 4, 7, 2, 15],
            [
                0.0,
                0.5,
                1.0,
                1.5,
                2.0,
                2.5,
                3.0,
                3.5,
                4.0,
                4.5,
                5.0,
                5.5,
                6.0,
                6.5,
                7.0,
                7.5,
                8.0,
                8.5,
                9.0,
                9.5,
            ],
        ],
        "movements_is_error": [
            [187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
            [
                0.0,
                0.05,
                0.1,
                0.15,
                0.2,
                0.25,
                0.3,
                0.35,
                0.4,
                0.45,
                0.5,
                0.55,
                0.6,
                0.65,
                0.7,
                0.75,
                0.8,
                0.85,
                0.9,
                0.95,
            ],
        ],
        "patient_id": math.nan,
        "room": [
            [96, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 94],
            [
                1.0,
                1.05,
                1.1,
                1.15,
                1.2,
                1.25,
                1.3,
                1.35,
                1.4,
                1.45,
                1.5,
                1.55,
                1.6,
                1.65,
                1.7,
                1.75,
                1.8,
                1.85,
                1.9,
                1.95,
            ],
        ],
        "rr": [
            [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 187],
            [
                5.0,
                6.0,
                7.0,
                8.0,
                9.0,
                10.0,
                11.0,
                12.0,
                13.0,
                14.0,
                15.0,
                16.0,
                17.0,
                18.0,
                19.0,
                20.0,
                21.0,
                22.0,
                23.0,
                24.0,
            ],
        ],
        "rr_is_error": [
            [187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
            [
                0.0,
                0.05,
                0.1,
                0.15,
                0.2,
                0.25,
                0.3,
                0.35,
                0.4,
                0.45,
                0.5,
                0.55,
                0.6,
                0.65,
                0.7,
                0.75,
                0.8,
                0.85,
                0.9,
                0.95,
            ],
        ],
        "spo2": [
            [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 187],
            [
                85.0,
                85.7,
                86.4,
                87.1,
                87.8,
                88.5,
                89.2,
                89.9,
                90.6,
                91.3,
                92.0,
                92.7,
                93.4,
                94.1,
                94.8,
                95.5,
                96.2,
                96.9,
                97.6,
                98.3,
            ],
        ],
        "spo2_is_error": [
            [187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
            [
                0.0,
                0.05,
                0.1,
                0.15,
                0.2,
                0.25,
                0.3,
                0.35,
                0.4,
                0.45,
                0.5,
                0.55,
                0.6,
                0.65,
                0.7,
                0.75,
                0.8,
                0.85,
                0.9,
                0.95,
            ],
        ],
        "timestamp": [
            [20, 0, 20, 0, 20, 0, 20, 0, 20, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 10],
            [
                "2020-12-01T17:24:15.910000+00:00",
                "2020-12-01T17:24:42.910000+00:00",
                "2020-12-01T17:25:09.910000+00:00",
                "2020-12-01T17:25:36.910000+00:00",
                "2020-12-01T17:26:03.910000+00:00",
                "2020-12-01T17:26:30.910000+00:00",
                "2020-12-01T17:26:57.910000+00:00",
                "2020-12-01T17:27:24.910000+00:00",
                "2020-12-01T17:27:51.910000+00:00",
                "2020-12-01T17:28:18.910000+00:00",
                "2020-12-01T17:28:45.910000+00:00",
                "2020-12-01T17:29:12.910000+00:00",
                "2020-12-01T17:29:39.910000+00:00",
                "2020-12-01T17:30:06.910000+00:00",
                "2020-12-01T17:30:33.910000+00:00",
                "2020-12-01T17:31:00.910000+00:00",
                "2020-12-01T17:31:27.910000+00:00",
                "2020-12-01T17:31:54.910000+00:00",
                "2020-12-01T17:32:21.910000+00:00",
                "2020-12-01T17:32:48.910000+00:00",
            ],
        ],
        "turn_count": [
            [71, 4, 3, 3, 4, 5, 3, 2, 4, 6, 3, 5, 6, 6, 1, 2, 2, 7, 4, 49],
            [
                0.0,
                0.15,
                0.3,
                0.45,
                0.6,
                0.75,
                0.9,
                1.05,
                1.2,
                1.35,
                1.5,
                1.65,
                1.8,
                1.95,
                2.1,
                2.25,
                2.4,
                2.55,
                2.7,
                2.85,
            ],
        ],
        "turn_count_is_error": [
            [187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3],
            [
                0.0,
                0.05,
                0.1,
                0.15,
                0.2,
                0.25,
                0.3,
                0.35,
                0.4,
                0.45,
                0.5,
                0.55,
                0.6,
                0.65,
                0.7,
                0.75,
                0.8,
                0.85,
                0.9,
                0.95,
            ],
        ],
    },
}
