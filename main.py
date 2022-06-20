import psycopg2
import json
from riotwatcher import TftWatcher

connection = psycopg2.connect('dbname=tft user=postgres password=jorge432')
cursor = connection.cursor()

region = "na1"
tft_region = "americas"
summoner_name = "Duddlez"
api_key = "RGAPI-bb107900-c5e3-4f9e-9ad8-1195d480e249"
watcher = TftWatcher(api_key)

tft_file = open('tft_data.json')
tft = json.load(tft_file)
items = tft['items']
match_dic = {}


def main(match_list):
    my_info = watcher.summoner.by_name(region, summoner_name)
    for match in match_list:
        match_data = watcher.match.by_id(tft_region, match)
        my_index = match_data['metadata']['participants'].index(my_info['puuid'])
        match_id = match_id_to_int(match_data['metadata']['match_id'])
        match_info = match_data['info']
        update_tables(my_index, match_id, match_info)
    print("Data successfully sent to pgAdmin 4.")


def get_item_name(item_id):
    for i in range(len(items)):
        if items[i]['id'] == item_id:
            return items[i]['name'], i


def match_id_to_int(match_id_string):
    match_id_int = match_id_string
    match_id_int = match_id_int[4:]
    return int(match_id_int)


def update_tables(my_index, match_id, match_info):
    # Gather data for 'matches' table
    version = match_info['game_version'][8:][:13]
    my_match_data = match_info['participants'][my_index]
    gold_left = my_match_data['gold_left']
    last_round = my_match_data['last_round']
    placement = my_match_data['placement']
    dmg = my_match_data['total_damage_to_players']
    update_matches_table(match_id, version, gold_left, last_round, placement, dmg)

    # Gather data for 'augments' table
    augments = [None, None, None]
    for index, augment in enumerate(my_match_data['augments']):
        augments[index] = augment
    update_augments_table(match_id, augments)

    # Gather data for 'traits' table
    for trait in my_match_data['traits']:
        update_traits_table(match_id, trait['name'], trait['num_units'], trait['tier_current'], trait['tier_total'])

    # Gather data for 'units' and 'items' tables
    # I do them together so I don't have to get items twice
    match_items = []
    for unit in my_match_data['units']:
        char_id = unit['character_id'][unit['character_id'].index('_') + 1:]
        item_count = len(unit['items'])
        unit_item_list = [(None, None), (None, None), (None, None)]
        if item_count > 0:
            for index, item in enumerate(unit['items']):
                unit_item_list[index] = get_item_name(item)
                match_items.append(unit_item_list[index])
        update_units_table(match_id, char_id, unit_item_list[0][0], unit_item_list[1][0], unit_item_list[2][0], unit['tier'])
    for item in match_items:
        item_from = items[item[1]]['from']
        if len(item_from) > 0:
            for from_id in item_from:
                match_items.append(get_item_name(from_id))
        update_items_table(match_id, item[0], len(item_from) > 0)


def update_matches_table(match_id, version, gold_left, last_round, placement, dmg):
    cursor.execute(
        'INSERT INTO matches(match_id, game_version, gold_left, last_round, placement, total_damage_to_players) VALUES (%s, %s, %s, %s, %s, %s)',
        (match_id, version, gold_left, last_round, placement, dmg))


def update_augments_table(match_id, augments):
    cursor.execute(
        'INSERT INTO augments(match_id, augment_one, augment_two, augment_three) VALUES (%s, %s, %s, %s)',
        (match_id, augments[0], augments[1], augments[2]))


def update_traits_table(match_id, name, num, current, total):
    cursor.execute(
        'INSERT INTO traits(match_id, trait_name, num_of_units, tier_current, tier_total) VALUES (%s, %s, %s, %s, %s)',
        (match_id, name, num, current, total))


def update_units_table(match_id, character_id, item_one, item_two, item_three, tier):
    cursor.execute(
        'INSERT INTO units(match_id, character_id, item_one, item_two, item_three, tier) VALUES (%s, %s, %s, %s, %s, %s)',
        (match_id, character_id, item_one, item_two, item_three, tier))


def update_items_table(match_id, item_name, component):
    cursor.execute(
        'INSERT INTO items(match_id, item_name, component) VALUES (%s, %s, %s)', (match_id, item_name, component))


match_list_one = [
    "NA1_4334172057",
    "NA1_4333622882",
    "NA1_4333528330",
    "NA1_4333361441",
    "NA1_4333096672",
    "NA1_4332942329",
    "NA1_4332859995",
    "NA1_4332505012",
    "NA1_4332026744",
    "NA1_4331952941",
    "NA1_4331502421",
    "NA1_4331072510",
    "NA1_4330301056",
    "NA1_4329915281",
    "NA1_4329520079",
    "NA1_4329243232",
    "NA1_4328963834",
    "NA1_4328434027",
    "NA1_4328259292",
    "NA1_4328110151",
    "NA1_4328033052",
    "NA1_4327968116",
    "NA1_4327608009",
    "NA1_4327433181",
    "NA1_4327440269",
    "NA1_4326728645",
    "NA1_4326647358",
    "NA1_4326335009",
    "NA1_4325881642",
    "NA1_4325826011",
    "NA1_4325075823",
    "NA1_4325091553",
    "NA1_4324910713",
    "NA1_4324867999",
    "NA1_4324610193",
    "NA1_4324485845",
    "NA1_4324238657",
    "NA1_4323664288",
    "NA1_4322847911",
    "NA1_4322294996",
    "NA1_4322028458",
    "NA1_4321985257",
    "NA1_4320931626",
    "NA1_4320691965",
    "NA1_4320257913",
    "NA1_4320177145",
    "NA1_4320032297",
    "NA1_4319732367",
    "NA1_4319083871",
    "NA1_4319016385",
    "NA1_4318545299"]
match_list_two = [
    "NA1_4317875711",
    "NA1_4317799513",
    "NA1_4317483958",
    "NA1_4317412190",
    "NA1_4317308707",
    "NA1_4317223302",
    "NA1_4316922000",
    "NA1_4316834953",
    "NA1_4316430366",
    "NA1_4316184316",
    "NA1_4316191319",
    "NA1_4315873408",
    "NA1_4315816499",
    "NA1_4315758472",
    "NA1_4315404706",
    "NA1_4315285047",
    "NA1_4315270864",
    "NA1_4314642729",
    "NA1_4314602850",
    "NA1_4314547420",
    "NA1_4313999726",
    "NA1_4313634900",
    "NA1_4313578522",
    "NA1_4313323816",
    "NA1_4313095761",
    "NA1_4312717576",
    "NA1_4312676745",
    "NA1_4312619058",
    "NA1_4312600150",
    "NA1_4312563851",
    "NA1_4312222306",
    "NA1_4311839123",
    "NA1_4311541804",
    "NA1_4311514461",
    "NA1_4310584049",
    "NA1_4310535269",
    "NA1_4310481591",
    "NA1_4310413039",
    "NA1_4309920312",
    "NA1_4309742151",
    "NA1_4309342776",
    "NA1_4309306254",
    "NA1_4309259621",
    "NA1_4308803259",
    "NA1_4308653058",
    "NA1_4308620209",
    "NA1_4308505891",
    "NA1_4308288416",
    "NA1_4308272825",
    "NA1_4308205245",
    "NA1_4307547759"]
match_list_three = [
    "NA1_4307582446",
    "NA1_4307519003",
    "NA1_4307524840",
    "NA1_4307469098",
    "NA1_4307215282",
    "NA1_4307210401",
    "NA1_4306223659",
    "NA1_4305720366",
    "NA1_4305619679",
    "NA1_4305348499",
    "NA1_4305149500",
    "NA1_4305135983",
    "NA1_4304885666",
    "NA1_4304804765",
    "NA1_4304758292",
    "NA1_4304742793",
    "NA1_4304686484",
    "NA1_4304710071",
    "NA1_4304384046",
    "NA1_4304216691",
    "NA1_4304224906",
    "NA1_4303630430",
    "NA1_4303554246",
    "NA1_4303544194",
    "NA1_4303122581",
    "NA1_4303094048",
    "NA1_4303092509",
    "NA1_4302713392",
    "NA1_4302682346",
    "NA1_4302526287",
    "NA1_4302149512",
    "NA1_4302154891",
    "NA1_4301976221",
    "NA1_4301906656",
    "NA1_4301632021",
    "NA1_4300573325",
    "NA1_4300029069",
    "NA1_4300046617",
    "NA1_4300035475",
    "NA1_4299681416",
    "NA1_4299003963",
    "NA1_4298673725",
    "NA1_4298654053",
    "NA1_4298587412",
    "NA1_4298539541",
    "NA1_4298137842",
    "NA1_4297832070",
    "NA1_4297803927",
    "NA1_4297724296",
    "NA1_4297697281",
    "NA1_4297076284"]
match_list_four = [
    "NA1_4296774905",
    "NA1_4296718014",
    "NA1_4296629222",
    "NA1_4296643246",
    "NA1_4296498841",
    "NA1_4296457018",
    "NA1_4295983093",
    "NA1_4295827496",
    "NA1_4295624396",
    "NA1_4295486599",
    "NA1_4295491599",
    "NA1_4295396803",
    "NA1_4295266035",
    "NA1_4295223454",
    "NA1_4294538260",
    "NA1_4294379180",
    "NA1_4294186471",
    "NA1_4293971200",
    "NA1_4293859273",
    "NA1_4293862179",
    "NA1_4293705849",
    "NA1_4293667129",
    "NA1_4293475868",
    "NA1_4293415247",
    "NA1_4293206889",
    "NA1_4292998646",
    "NA1_4292993057",
    "NA1_4292642687",
    "NA1_4292202170",
    "NA1_4292158811",
    "NA1_4292028423",
    "NA1_4291842866",
    "NA1_4291776208",
    "NA1_4291569091",
    "NA1_4291532787",
    "NA1_4291366548",
    "NA1_4291401587",
    "NA1_4291256907",
    "NA1_4291228661",
    "NA1_4291205945",
    "NA1_4291214151",
    "NA1_4290972235",
    "NA1_4290573392",
    "NA1_4290529644",
    "NA1_4290252609",
    "NA1_4289374133",
    "NA1_4288505103",
    "NA1_4288361750",
    "NA1_4288277235",
    "NA1_4288245224",
    "NA1_4287869334"]
match_list_five = [
    "NA1_4286919258",
    "NA1_4285676509",
    "NA1_4285671238",
    "NA1_4284487717",
    "NA1_4283214421",
    "NA1_4283105490",
    "NA1_4283059686",
    "NA1_4282929423",
    "NA1_4282956103",
    "NA1_4282802889",
    "NA1_4282765194",
    "NA1_4282742259",
    "NA1_4282677236",
    "NA1_4281507330",
    "NA1_4281334516",
    "NA1_4281340994",
    "NA1_4281290144",
    "NA1_4281108069",
    "NA1_4281122891",
    "NA1_4279978928",
    "NA1_4279928002",
    "NA1_4279417408",
    "NA1_4277881101",
    "NA1_4277321526",
    "NA1_4277252901",
    "NA1_4276477044",
    "NA1_4276491616",
    "NA1_4275365533",
    "NA1_4275259668",
    "NA1_4275273205",
    "NA1_4272920538",
    "NA1_4270231687",
    "NA1_4269435091",
    "NA1_4267431807",
    "NA1_4267123866",
    "NA1_4267054638",
    "NA1_4267032130",
    "NA1_4266833950",
    "NA1_4262000850",
    "NA1_4261947606",
    "NA1_4259944703",
    "NA1_4259941516",
    "NA1_4259798941",
    "NA1_4258814734",
    "NA1_4256254176",
    "NA1_4255382685",
    "NA1_4253357991",
    "NA1_4253346102",
    "NA1_4252627401",
    "NA1_4252631763",
    "NA1_4252405049"]
match_list_six = [
    "NA1_4252401174",
    "NA1_4251129373",
    "NA1_4251156170",
    "NA1_4249748353",
    "NA1_4248684046",
    "NA1_4248617599",
    "NA1_4246955183",
    "NA1_4246897613",
    "NA1_4246894730",
    "NA1_4245738650",
    "NA1_4244093939",
    "NA1_4243014496",
    "NA1_4240261078",
    "NA1_4240188907",
    "NA1_4240138529",
    "NA1_4240087490",
    "NA1_4239192311",
    "NA1_4239180104",
    "NA1_4238686059",
    "NA1_4236665447",
    "NA1_4236579613",
    "NA1_4235689339",
    "NA1_4234715444",
    "NA1_4234692457",
    "NA1_4233609723",
    "NA1_4233537953",
    "NA1_4233438818",
    "NA1_4233416950",
    "NA1_4232371625",
    "NA1_4232208163",
    "NA1_4232206507",
    "NA1_4232194762",
    "NA1_4226767834",
    "NA1_4226792466",
    "NA1_4226728814",
    "NA1_4226638598",
    "NA1_4226664629",
    "NA1_4226615831",
    "NA1_4225846102",
    "NA1_4224142308",
    "NA1_4224069385",
    "NA1_4222996879",
    "NA1_4223002430",
    "NA1_4222948013",
    "NA1_4219784307",
    "NA1_4219738334",
    "NA1_4219702876"
]

main(match_list_six)

connection.commit()
