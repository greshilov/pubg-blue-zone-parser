import dataclasses
import requests
import typing

from collections import Counter
from multiprocessing import Pool

Point = typing.Tuple[float, float]  # x, y
Circle = typing.Tuple[float, float, float]  # x, y, r


@dataclasses.dataclass
class BzParams:
    point_a: Point
    point_b: Point

    zones: Circle


def extract_bz_params(replay_url: str) -> BzParams:
    telemetry = requests.get(replay_url)
    events = telemetry.json()
    safe_zones = list()

    saved_points_of_chosen_account = list()

    for event in events:
        if event['_T'] == 'LogGameStatePeriodic':
            safe_zones.append(
                (event['gameState']['safetyZonePosition']['x'],
                 event['gameState']['safetyZonePosition']['y'],
                 event['gameState']['safetyZoneRadius'])
            )
        elif event['_T'] == 'LogPlayerPosition' and 0 < event['common']['isGame'] < 0.5 and len(saved_points_of_chosen_account) < 2:
            point = (event['character']['location']['x'], event['character']['location']['y'])
            if point not in saved_points_of_chosen_account:
                saved_points_of_chosen_account.append(
                    (event['character']['location']['x'],
                     event['character']['location']['y'])
                )

    zones = tuple(zone for zone, count in Counter(safe_zones).most_common() if count > 1)
    point_a, point_b = saved_points_of_chosen_account
    return BzParams(
        point_a=point_a,
        point_b=point_b,
        zones=zones,
    )


def extract_bz_params_mp(replay_url_list: typing.Iterable):
    for bz_params in Pool(processes=8).imap_unordered(extract_bz_params, replay_url_list):
        yield bz_params


if __name__ == '__main__':
    example_telemetry_urls = (
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/21/18/7102ad18-ce90-11e9-a9c4-0a586469f018-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/17/42/3a1c00cf-ce72-11e9-9346-0a586467d463-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/17/24/a3e28c32-ce6f-11e9-a981-0a5864687231-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/19/23/47dc322c-ce80-11e9-b67a-0a586469ea15-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/17/53/ae1e5f07-ce73-11e9-9346-0a586467d463-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/12/14/7043514f-ce44-11e9-9346-0a586467d463-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/18/27/6bc442f6-ce78-11e9-960c-0a586468b388-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/17/16/836f23fd-ce6e-11e9-960c-0a586468b388-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/13/08/e8659c57-ce4b-11e9-a6c3-0a586467e77f-telemetry.json',
        'https://telemetry-cdn.playbattlegrounds.com/bluehole-pubg/steam/2019/09/03/12/40/0d277828-ce48-11e9-a6c3-0a586467e77f-telemetry.json',
    )
    for bz_param in extract_bz_params_mp(example_telemetry_urls):
        print(bz_param)
