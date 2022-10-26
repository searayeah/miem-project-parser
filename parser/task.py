import requests
import os
import pandas as pd
import numpy as np
import gspread
from custom_exceptions import APIException
import schedule
import time

API = "https://cabinet.miem.hse.ru/public-api/"
NAMES = ["Слава", "Ваня", "Андрей", "Даня", "Вова", "Миша", "Антон"]
SHEET_KEY = os.environ["sheet_key"]
SHEET_NAME = "sheet1"
TOKEN_NAMES_LIST = [
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
    "auth_provider_x509_cert_url",
    "client_x509_cert_url",
]
TOKEN = {item: os.environ[item].replace("\\n", "\n") for item in TOKEN_NAMES_LIST}


class Parser:
    def __init__(self, api, token, sheet_key, sheet_name):
        self.api = api
        self.token = token
        self.sheet_key = sheet_key
        self.sheet_name = sheet_name

        self.df_new = None
        self.df_old = None

        self.worksheet = None

    def _process_response_data(self, df_projects, df_sandbox):
        diff = np.setdiff1d(df_sandbox["id"].to_numpy(), df_projects["id"].to_numpy())
        df_projects.set_index("id", inplace=True, drop=False)
        df_sandbox.set_index("id", inplace=True, drop=False)

        self.df_new = pd.concat([df_projects, df_sandbox.loc[diff]])
        self.df_new.fillna("", inplace=True)

        eng_columns = [
            "id",
            "nameRus",
            "typeDesc",
            "head",
            "vacancyData",
        ]
        self.df_new = self.df_new[eng_columns]

        self.df_new["vacancyData"] = self.df_new["vacancyData"].apply(
            lambda x: ", ".join(x)
        )
        self.df_new["Год"] = self.df_new["id"].apply(self._get_proj_year)
        year_ordering = [
            "2021/2022 учебный год",
            "2020/2021 учебный год",
            "2019/2020 учебный год",
        ]
        self.df_new["Год"] = pd.Categorical(
            self.df_new["Год"], categories=year_ordering, ordered=True
        )
        self.df_new["id"] = self.df_new["id"].apply(
            lambda x: f'=HYPERLINK("https://cabinet.miem.hse.ru/#/project/{x}/", {x})'
        )

        self.df_new[["Годность"] + NAMES + ["Коментарий"]] = ""
        rus_columns = (
            [
                "id",
                "Название",
                "Тип",
                "Руководитель",
                "Вакансии",
                "Год",
                "Годность",
            ]
            + NAMES
            + ["Коментарий"]
        )

        self.df_new.columns = rus_columns

    def _process_old_data(self):
        self.df_old = pd.DataFrame(self.worksheet.get_all_records())
        self.df_old.set_index("id", inplace=True, drop=False)

        index = self.df_old.index.to_numpy()

        values = {name: self.df_old[name].to_numpy() for name in NAMES + ["Коментарий"]}

        new_index = self.df_new.index.to_numpy()
        for i in range(len(index)):
            if index[i] in new_index:
                for name in NAMES + ["Коментарий"]:
                    self.df_new.at[index[i], name] = values[name][i]

        types_ordering = ["Прогр.", "Стартап", "Прогр-аппарат.", "НИР"]
        self.df_new["Тип"] = pd.Categorical(
            self.df_new["Тип"], categories=types_ordering, ordered=True
        )
        self.df_new.sort_values(by=["Год", "Тип"], inplace=True)
        range_ = [f"=AVERAGE(H{x}:N{x})" for x in range(2, len(self.df_new) + 2)]
        self.df_new["Годность"] = range_

    def get_new(self):
        response_projects = requests.get(API + "projects").json()
        if (
            response_projects["message"] != "success"
            and response_projects["message"] != "OK"
        ):
            raise APIException("MIEM API and servers are trash. Response =! OK")

        response_sandbox = requests.get(API + "sandbox").json()
        if (
            response_sandbox["message"] != "success"
            and response_sandbox["message"] != "OK"
        ):
            raise APIException("MIEM API and servers are trash. Response =! OK")

        df_projects = pd.DataFrame(response_projects["data"])
        df_sandbox = pd.DataFrame(response_sandbox["data"])

        self._process_response_data(df_projects, df_sandbox)

    def _get_proj_year(self, project_id):
        response = requests.get(API + "project/header/" + str(project_id)).json()
        if response["message"] != "success" and response["message"] != "OK":
            raise APIException(
                "Error in '_get_prof_year'. MIEM API and servers are trash. Or the project_id not found"
            )
        try:
            return response["data"]["years"][-1]["year"]
        except Exception as error:
            raise APIException(
                "Error in '_get_prof_year'. Year not found in json response",
                error,
            )

    def _get_sheet(self):
        gc = gspread.service_account_from_dict(self.token)
        sh = gc.open_by_key(self.sheet_key)
        self.worksheet = getattr(sh, self.sheet_name)

    def get_old(self):
        self._get_sheet()
        self._process_old_data()

    def post_data(self):
        self.worksheet.resize(rows=2, cols=15)
        self.worksheet.clear()
        self.worksheet.update(
            [self.df_new.columns.values.tolist()] + self.df_new.values.tolist(),
            value_input_option="USER_ENTERED",
        )


def main():
    try:
        parser = Parser(API, TOKEN, SHEET_KEY, SHEET_NAME)

        attempts = 3
        for i in range(attempts):
            try:
                parser.get_new()
                break
            except APIException as error:
                print(error, "retrying...")
                pass
        else:
            raise APIException(
                f"MIEM API and servers are trash. Failed request {attempts} times in a row"
            )

        parser.get_old()
        parser.post_data()
        print("Completed successfully")

    except Exception as error:
        print(error)
        print("Time will heal wounds, retrying in one day...")


if __name__ == "__main__":
    # schedule.every(5).seconds.do(main)
    schedule.every().day.at("21:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
