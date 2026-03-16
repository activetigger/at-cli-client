import io
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd  # type: ignore[import]
import requests  # type: ignore[import]
import yaml  # type: ignore[import]
from requests.packages.urllib3.exceptions import (  # type: ignore[import]
    InsecureRequestWarning,
)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class AtApi:
    def __init__(self, url: str | None = None, config: str | None = None):
        """
        Initialize the client
        """
        self.headers: dict[str, str] | None = None
        if config:
            if not Path(config).is_file():
                raise Exception("Config file does not exist")
            with open(Path(config), "r") as stream:
                config_data = yaml.load(stream, Loader=yaml.FullLoader)
            if "url" not in config_data:
                raise Exception("Url not found in config file")
            self.url = config_data.get("url")
            if "username" in config_data and "password" in config_data:
                self.connect(config_data.get("username"), config_data.get("password"))
        else:
            if not url:
                raise Exception("Url not provided")
            self.url = url

    def _parse_error(self, r: requests.Response) -> str:
        """Extract a readable error message from an error response."""
        try:
            data = r.json()
            if isinstance(data, dict) and "detail" in data:
                detail = data["detail"]
                if isinstance(detail, list):
                    return "; ".join(
                        f"{'.'.join(str(loc) for loc in e.get('loc', []))}: {e.get('msg', '')}"
                        for e in detail
                    )
                return str(detail)
        except Exception:
            pass
        return r.text or f"HTTP {r.status_code}"

    def ping(self) -> dict:
        """
        Test API availability and measure response time.

        Returns a dict with:
            - available (bool): whether the API responded successfully
            - response_time_ms (float | None): round-trip time in milliseconds
            - status_code (int | None): HTTP status code returned
            - timestamp (str): ISO timestamp of the check
        """
        result = {
            "available": False,
            "response_time_ms": None,
            "status_code": None,
            "timestamp": datetime.utcnow().isoformat(),
        }
        try:
            start = time.monotonic()
            r = requests.get(f"{self.url}/", verify=False, timeout=10)
            result["response_time_ms"] = round((time.monotonic() - start) * 1000, 2)
            result["status_code"] = r.status_code
            result["available"] = r.status_code == 200
        except requests.exceptions.RequestException:
            pass
        return result

    def connect(self, username: str, password: str):
        """
        Get token access with username/password
        """
        try:
            response = requests.post(
                f"{self.url}/token",
                data={"username": username, "password": password},
                verify=False,
            )
            response.raise_for_status()
            token_data = response.json()
        except requests.exceptions.HTTPError:
            print(f"Error connecting: {self._parse_error(response)}")
            return
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to endpoint: {e}")
            return
        access_token = token_data.get("access_token")
        if access_token:
            self.headers = {
                "Authorization": f"Bearer {access_token}",
                "username": username,
            }
            print("Token received")
        else:
            print("Error: no access token in response")

    def get_project_state(self, project_slug: str):
        """
        Get project state
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(
            f"{self.url}/projects/{project_slug}", headers=self.headers, verify=False
        )
        if not r.ok:
            raise Exception(f"Error getting project state: {self._parse_error(r)}")
        return r.json()

    def get_projects(self):
        """
        Get projects
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(f"{self.url}/projects", headers=self.headers, verify=False)
        if not r.ok:
            raise Exception(f"Error getting projects: {self._parse_error(r)}")
        return r.json()["projects"]

    def get_projects_slugs(self):
        """
        Get projects slugs
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(f"{self.url}/projects", headers=self.headers, verify=False)
        if not r.ok:
            raise Exception(f"Error getting projects: {self._parse_error(r)}")
        return [i["parameters"]["project_slug"] for i in r.json()["projects"]]

    def add_project(
        self,
        project_name: str,
        data: pd.DataFrame,
        col_id: str,
        cols_text: List[str],
        cols_context: List[str] = [],
        cols_label: List[str] = [],
        n_train: int = 500,
        n_test: int = 0,
        n_valid: int = 0,
        filename: str = "data.csv",
        language: str = "fr",
        random_selection: bool = False,
        n_skip: int = 0,
        default_scheme: List[str] = [],
        embeddings: List[str] = [],
        test: bool = False,
        valid: bool = False,
        n_total: int | None = None,
        clear_test: bool = False,
        clear_valid: bool = False,
        cols_stratify: List[str] = [],
        stratify_train: bool = False,
        stratify_test: bool = False,
        force_label: bool = False,
        force_computation: bool = False,
        seed: int = 42,
        from_project: str | None = None,
        from_toy_dataset: bool = False,
    ):
        """
        Create a new project

        Args:
            project_name: name of the project
            data: data to use as a Pandas DataFrame
            col_id: column id
            cols_text: list of text columns
            cols_context: list of context columns
            cols_label: list of label columns
            n_train: number of training samples
            n_test: number of test samples
            n_valid: number of validation samples
            filename: name of the uploaded file
            language: language of the project
            random_selection: whether to randomly select samples
            n_skip: number of samples to skip
            default_scheme: default annotation scheme labels
            embeddings: list of embeddings to compute
            test: whether to create a test split
            valid: whether to create a validation split
            n_total: total number of samples (optional)
            clear_test: whether to clear the existing test set
            clear_valid: whether to clear the existing validation set
            cols_stratify: columns to use for stratification
            stratify_train: whether to stratify the training set
            stratify_test: whether to stratify the test set
            force_label: whether to force label assignment
            force_computation: whether to force recomputation
            seed: random seed
            from_project: slug of an existing project to copy data from
            from_toy_dataset: whether to use a toy dataset
        """

        if not self.headers:
            raise Exception("No token found")

        # test if the elements exist
        if col_id not in data.columns:
            raise Exception(f"Column {col_id} not found in data")
        for col_text in cols_text:
            if col_text not in data.columns:
                raise Exception(f"Column {col_text} not found in data")
        for col_context in cols_context:
            if col_context not in data.columns:
                raise Exception(f"Column {col_context} not found in data")
        for col_label in cols_label:
            if col_label not in data.columns:
                raise Exception(f"Column {col_label} not found in data")

        # send the file
        csv_string = data.to_csv(index=False)
        r = requests.post(
            f"{self.url}/files/add/project",
            params={"project_name": project_name},
            files={"file": (filename, csv_string)},
            verify=False,
            headers=self.headers,
        )
        if not r.ok:
            raise Exception(f"Error uploading file: {self._parse_error(r)}")

        # create the project
        form = {
            "project_name": project_name,
            "col_id": col_id,
            "cols_text": cols_text,
            "cols_label": cols_label,
            "filename": filename,
            "cols_context": cols_context,
            "language": language,
            "n_train": n_train,
            "n_test": n_test,
            "n_valid": n_valid,
            "random_selection": random_selection,
            "n_skip": n_skip,
            "default_scheme": default_scheme,
            "embeddings": embeddings,
            "test": test,
            "valid": valid,
            "n_total": n_total,
            "clear_test": clear_test,
            "clear_valid": clear_valid,
            "cols_stratify": cols_stratify,
            "stratify_train": stratify_train,
            "stratify_test": stratify_test,
            "force_label": force_label,
            "force_computation": force_computation,
            "seed": seed,
            "from_project": from_project,
            "from_toy_dataset": from_toy_dataset,
        }

        r = requests.post(
            f"{self.url}/projects/new", json=form, headers=self.headers, verify=False
        )
        if not r.ok:
            raise Exception(f"Error creating project: {self._parse_error(r)}")
        return r.json()

    def delete_project(self, project_slug: str):
        """
        Delete a project
        """
        if not self.headers:
            raise Exception("No token found")

        r = requests.post(
            f"{self.url}/projects/delete",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
        )
        if not r.ok:
            print(f"Error deleting project: {self._parse_error(r)}")
        else:
            print("Project deleted")

    def close_project(self, project_slug: str):
        """
        Close a project from memory
        """
        if not self.headers:
            raise Exception("No token found")

        r = requests.post(
            f"{self.url}/projects/close/{project_slug}",
            headers=self.headers,
            verify=False,
        )
        if not r.ok:
            print(f"Error closing project: {self._parse_error(r)}")
        else:
            print(f"Project {project_slug} closed")

    def get_users(self):
        """
        Get users
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(f"{self.url}/users", headers=self.headers, verify=False)
        if not r.ok:
            raise Exception(f"Error getting users: {self._parse_error(r)}")
        return r.json()

    def add_user(
        self, username: str, password: str, mail: str, status: str = "manager"
    ):
        """
        Create a new user
        """
        if not self.headers:
            raise Exception("No token found")

        r = requests.post(
            f"{self.url}/users/create",
            json={
                "username": username,
                "password": password,
                "contact": mail,
                "status": status,
            },
            headers=self.headers,
            verify=False,
        )
        if not r.ok:
            print(f"Error creating user: {self._parse_error(r)}")
        else:
            print("User created")

    def delete_user(self, username: str) -> None:
        """
        Delete a user
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/users/delete",
            headers=self.headers,
            verify=False,
            params={"user_to_delete": username},
        )
        if not r.ok:
            print(f"Error deleting user: {self._parse_error(r)}")
        else:
            print("User deleted")

    def get_annotations_data(
        self,
        project_slug: str,
        scheme: str,
        dataset: str = "train",
        verbose: bool = False,
    ):
        """
        Get current annotations for a projet/scheme
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(
            f"{self.url}/export/data",
            params={
                "project_slug": project_slug,
                "scheme": scheme,
                "dataset": dataset,
                "format": "csv",
            },
            headers=self.headers,
            verify=False,
        )
        if not r.ok:
            if verbose:
                print(f"Error getting annotations: {self._parse_error(r)}")
            return None
        try:
            csv_decoded = r.content.decode("utf-8")
            csv_io = io.StringIO(csv_decoded)
            t = pd.read_csv(csv_io)
            if len(t) == 0:
                if verbose:
                    print(f"No {dataset} annotations found for {project_slug}/{scheme}")
                return None
            return t
        except Exception as e:
            if verbose:
                print(f"Error parsing annotations: {e}")
            return None

    def add_auth_user_project(
        self, username: str, project_slug: str, auth: str = "manager"
    ):
        """
        Add a user to a project
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/users/auth/add",
            headers=self.headers,
            verify=False,
            json={"project_slug": project_slug, "username": username, "status": auth},
        )
        if not r.ok:
            print(f"Error adding user auth: {self._parse_error(r)}")
        else:
            print("Auth added to user")

    def delete_auth_user_project(self, username: str, project_slug: str):
        """
        Delete a user from a project
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/users/auth/delete",
            headers=self.headers,
            verify=False,
            json={"project_slug": project_slug, "username": username},
        )
        if not r.ok:
            print(f"Error deleting user auth: {self._parse_error(r)}")
        else:
            print("Auth deleted for user")

    def get_features(self, project_slug: str):
        """
        Get features of the project
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(
            f"{self.url}/features/available",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
        )
        if not r.ok:
            raise Exception(f"Error getting features: {self._parse_error(r)}")
        return r.json()

    def add_feature(
        self,
        project_slug: str,
        feature_name: str,
        feature_type: str,
        feature_parameters: dict = {},
    ):
        """
        Train a feature
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/features/add",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
            json={
                "name": feature_name,
                "type": feature_type,
                "parameters": feature_parameters,
            },
        )
        if not r.ok:
            print(f"Error adding feature: {self._parse_error(r)}")
        else:
            print("Feature in training")

    def get_features_data(
        self, project_slug: str, features: list[str], format: str = "csv"
    ):
        """
        Get features from a project
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(
            f"{self.url}/export/features",
            params={
                "project_slug": project_slug,
                "features": features,
                "format": format,
            },
            headers=self.headers,
            verify=False,
        )
        if not r.ok:
            raise Exception(f"Error getting features data: {self._parse_error(r)}")
        try:
            csv_decoded = r.content.decode("utf-8")
            csv_io = io.StringIO(csv_decoded)
            return pd.read_csv(csv_io)
        except Exception as e:
            raise Exception(f"Error parsing features data: {e}")

    def get_schemes(self, project_slug: str):
        """
        Get schemes of a project
        """
        if not self.headers:
            raise Exception("No token found")
        r = self.get_project_state(project_slug)
        if "schemes" in r:
            return r["schemes"]["available"]
        else:
            raise Exception(f"No schemes found for project {project_slug}")

    def add_scheme_to_project(
        self,
        project_slug: str,
        scheme: str,
        labels: str | None = None,
        kind: str = "multiclass",
    ):
        """
        Add a scheme to a project
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/schemes/add",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
            json={
                "project_slug": project_slug,
                "name": scheme,
                "kind": kind,
                "labels": labels,
            },
        )
        if not r.ok:
            print(f"Error adding scheme: {self._parse_error(r)}")
        else:
            print("Scheme added to project")

    def delete_scheme_from_project(self, project_slug: str, scheme: str):
        """
        Delete a scheme from a project
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/schemes/delete",
            headers=self.headers,
            params={"project_slug": project_slug},
            verify=False,
            json={
                "project_slug": project_slug,
                "name": scheme,
                "kind": "",
                "labels": [],
            },
        )
        if not r.ok:
            print(f"Error deleting scheme: {self._parse_error(r)}")
        else:
            print("Scheme deleted from project")

    def add_label_to_scheme(self, project_slug: str, scheme: str, label: str):
        """
        Add a label to a scheme
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/schemes/label/add",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug, "scheme": scheme, "label": label},
        )
        if not r.ok:
            print(f"Error adding label: {self._parse_error(r)}")
        else:
            print("Label added to scheme")

    def delete_label_from_scheme(self, project_slug: str, scheme: str, label: str):
        """
        Delete a label from a scheme
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/schemes/label/delete",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug, "scheme": scheme, "label": label},
        )
        if not r.ok:
            print(f"Error deleting label: {self._parse_error(r)}")
        else:
            print("Label deleted from scheme")

    def download_raw_dataset(self, project_slug: str, folder: str = "./"):
        """
        Download raw dataset
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.get(
            f"{self.url}/export/raw",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
        )
        if not r.ok:
            raise Exception(f"Error downloading raw dataset: {self._parse_error(r)}")
        try:
            data = r.json()
            response = requests.get(
                f"{self.url}/{data['path']}", stream=True, verify=False
            )
            if not response.ok:
                raise Exception(f"Error fetching file: {self._parse_error(response)}")
            with open(f"{folder}/{data['name']}", "wb") as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)
        except Exception as e:
            raise Exception(f"Error downloading raw dataset: {e}")

    def export_project(
        self, project_slug: str, path: str = "./exports", raw_datasets: bool = False
    ):
        """
        Save a project
        for each scheme :
            - save train/test/valid annotations
        """
        if not self.headers:
            raise Exception("No token found")

        print(f"Starting the export of project {project_slug}")

        # create folder
        if Path(f"{path}/{project_slug}").exists():
            print(
                "This project seems already be saved, check or delete the previous version"
            )
            return None

        # create the folder
        os.makedirs(f"{path}/{project_slug}")
        path_project = f"{path}/{project_slug}"

        # get the state of the project to save it
        state = self.get_project_state(project_slug)
        with open(f"{path_project}/{project_slug}.json", "w") as f:
            json.dump(state, f)

        # get schemes annotation for each scheme
        schemes = self.get_schemes(project_slug)
        for scheme in schemes:
            for dataset in ["train", "test", "valid"]:
                t = self.get_annotations_data(project_slug, scheme, dataset)
                if t is not None:
                    t.to_csv(
                        f"{path_project}/annotations-scheme-{scheme}-{dataset}.csv"
                    )

        print(f"Project {project_slug} saved with {len(schemes)} schemes")

        # if requested, export raw dataset
        if raw_datasets:
            self.download_raw_dataset(project_slug, path_project)
            print("Raw dataset downloaded")

        # close the project to free server memory
        self.close_project(project_slug)

        return None

    def export_all(
        self,
        path: str = "./exports",
        raw_datasets: bool = False,
        since: datetime | None = None,
    ):
        """
        Save all the data

        Filter from the last activity
        """
        # get project summary
        projects = self.get_projects()

        # filter with the last activity
        if since is not None:
            projects = [
                project
                for project in projects
                if pd.to_datetime(project["last_activity"]) >= since
            ]

        # save each project
        for project in projects:
            self.export_project(project["project_slug"], path, raw_datasets)

    def get_models(self, project_slug: str) -> dict:
        """
        Get model state
        """
        r = self.get_project_state(project_slug)
        return {
            "available": r["bertmodels"]["available"],
            "training": r["bertmodels"]["training"],
        }

    def stop_finetune_model(self, project_slug: str):
        """
        Stop training a model
        """
        if not self.headers:
            raise Exception("No token found")
        r = requests.post(
            f"{self.url}/stop",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
        )
        if not r.ok:
            print(f"Error stopping model: {self._parse_error(r)}")
        else:
            print("Model stopped")

    def start_finetune_model(
        self,
        project_slug: str,
        scheme: str,
        name: str,
        base_model: str,
        params: dict | None = None,
        test_size: float = 0.2,
        dichotomize: str | None = None,
        class_min_freq: int = 1,
        class_balance: bool = False,
    ):
        """
        Start training a model
        """
        if not self.headers:
            raise Exception("No token found")

        if params is None:
            params = {
                "batchsize": 4,
                "gradacc": 1,
                "epochs": 3,
                "lrate": 5e-05,
                "wdecay": 0.01,
                "best": True,
                "eval": 10,
                "gpu": True,
                "adapt": True,
            }

        payload = {
            "project_slug": project_slug,
            "scheme": scheme,
            "name": name,
            "base_model": base_model,
            "params": params,
            "test_size": test_size,
            "dichotomize": dichotomize,
            "class_min_freq": class_min_freq,
            "class_balance": class_balance,
        }

        r = requests.post(
            f"{self.url}/models/bert/train",
            headers=self.headers,
            verify=False,
            params={"project_slug": project_slug},
            json=payload,
        )
        if not r.ok:
            print(f"Error starting model training: {self._parse_error(r)}")
        else:
            print("Model in training")
