import json

from sqlalchemy import create_engine, text
from datetime import datetime

with open("C:\\Projects\\TestMaterials\\config.json", "r") as f:
    conf = json.loads(f.read())

engine = create_engine(conf["AS_CONNECTION"])
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def logs_isn():
    with engine.connect() as connection:
        query = f"""SELECT fISN FROM TradeDB_I.dbo.LOGS WHERE fTYPE = 'MATERIAL' AND fSTATUS = 0 ORDER BY fISN"""
        result = connection.execute(text(query))
        materials = result.mappings().all()
        return materials


def select_materials():
    list_isn = logs_isn()
    if len(list_isn) > 0:
        data = []
        for isn in list_isn:
            with engine.connect() as connection:
                result = connection.execute(text(f"""SELECT
                                                            fMTCODE,
                                                            fCAPTION,
                                                            fMTISN
                                                        FROM
                                                            TradeDB.dbo.MATERIALS
                                                        WHERE
                                                            fMTISN = '{isn["fISN"]}'"""))
                material = dict(zip(result.keys(), result.fetchone()))
                data.append(material)

        return data


def send_materials():
    data = select_materials()
    if len(data) > 0:
        for material in data:
            print(material)
            # ենթադրենք այստեղ տվյալները ւղարկում ենք ինչ որ api ի և ստանում ենք 200 response

            with engine.begin() as begin:
                update_query = f"""UPDATE TradeDB_I.dbo.LOGS SET fSTATUS = '200', fINTDATE = '{current_datetime}', fRESPONSE = 'updated successfully' WHERE fISN = '{material["fMTISN"]}'"""
                begin.execute(text(update_query))


if __name__ == "__main__":
    send_materials()
