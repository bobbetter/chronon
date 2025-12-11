#!/usr/bin/env python3
"""
run.py needs to only depend in python standard library to simplify execution requirements.
"""

#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import argparse
import json
import logging
import os
import subprocess
from datetime import datetime

OFFLINE_ARGS = "--conf-path={conf_path} --end-date={ds} "
ONLINE_ARGS = "--online-jar={online_jar} --online-class={online_class} "
ONLINE_OFFLINE_WRITE_ARGS = OFFLINE_ARGS + ONLINE_ARGS

SPARK_MODES = [
    "backfill",
    "upload",
    "upload-to-kv",
]

MODE_ARGS = {
    "backfill": OFFLINE_ARGS,
    "upload": OFFLINE_ARGS,
    "upload-to-kv": ONLINE_OFFLINE_WRITE_ARGS,
}

ROUTES = {
    "group_bys": {
        "upload": "group-by-upload",
        "upload-to-kv": "group-by-upload-bulk-load",
        "backfill": "group-by-backfill",
    },
    "joins": {
        "backfill": "join",
    },
    "staging_queries": {
        "backfill": "staging-query-backfill",
    },
}

APP_NAME_TEMPLATE = "chronon_{conf_type}_{mode}_{name}"


def custom_json(conf):
    """Extract the json stored in customJson for a conf."""
    if conf.get("metaData", {}).get("customJson"):
        return json.loads(conf["metaData"]["customJson"])
    return {}


def check_call(cmd):
    print("Running command: " + cmd)
    return subprocess.check_call(cmd.split(), bufsize=0)


def set_runtime_env(args):
    """
    Setting the runtime environment variables based on the conf file.
    """
    conf_type = None
    app_name = None

    if args.conf and args.mode:
        try:
            _, conf_type, _, _ = args.conf.split("/")[-4:]
        except Exception as e:
            logging.error(
                "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                    args.conf
                )
            )
            raise e

        if os.path.isfile(args.conf):
            with open(args.conf, "r") as conf_file:
                conf_json = json.load(conf_file)
            # Load additional args from customJson
            if custom_json(conf_json) and args.mode in {"backfill", "upload", "upload-to-kv"}:
                additional_args = " ".join(custom_json(conf_json).get("additional_args", []))
                if additional_args:
                    os.environ["CHRONON_CONFIG_ADDITIONAL_ARGS"] = additional_args
            app_name = APP_NAME_TEMPLATE.format(
                mode=args.mode,
                conf_type=conf_type,
                name=conf_json["metaData"]["name"],
            )

    if args.app_name:
        app_name = args.app_name
    elif not app_name:
        # Provide basic app_name when no conf is defined.
        app_name = "_".join(
            [
                k
                for k in [
                    "chronon",
                    conf_type,
                    args.mode.replace("-", "_") if args.mode else None,
                ]
                if k is not None
            ]
        )

    if app_name:
        os.environ["APP_NAME"] = app_name
        print(f"Setting APP_NAME={app_name}")


class Runner:
    def __init__(self, args):
        self.conf = args.conf
        self.mode = args.mode

        if self.conf:
            try:
                _, self.conf_type, _, _ = self.conf.split("/")[-4:]
            except Exception as e:
                logging.error(
                    "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                        self.conf
                    )
                )
                raise e
            possible_modes = list(ROUTES[self.conf_type].keys())
            assert args.mode in possible_modes, "Invalid mode:{} for conf:{} of type:{}, please choose from {}".format(
                args.mode, self.conf, self.conf_type, possible_modes
            )
        else:
            self.conf_type = args.conf_type
        self.ds = args.end_ds if args.end_ds else args.ds
        self.start_ds = args.start_ds if args.start_ds else None
        self.args = args.args if args.args else ""
        self.spark_submit = args.spark_submit_path
        self.jar_path = os.environ.get("CHRONON_DRIVER_JAR")
        self.online_jar = os.environ.get("CHRONON_ONLINE_JAR")
        self.online_class = os.environ.get("CHRONON_ONLINE_CLASS")
        assert self.jar_path and os.path.exists(self.jar_path), (
            "CHRONON_DRIVER_JAR environment variable must be set and point to an existing file"
        )

    def run(self):
        command = (
            "bash {script} --class ai.chronon.spark.Driver {jar} {subcommand} {args} {additional_args}"
        ).format(
            script=self.spark_submit,
            jar=self.jar_path,
            subcommand=ROUTES[self.conf_type][self.mode],
            args=self._gen_final_args(),
            additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
        )
        check_call(command)

    def _gen_final_args(self):
        base_args = MODE_ARGS[self.mode].format(
            conf_path=self.conf,
            ds=self.ds,
            online_jar=self.online_jar,
            online_class=self.online_class,
        )
        override_start_partition_arg = " --start-partition-override=" + self.start_ds if self.start_ds else ""
        final_args = base_args + " " + str(self.args) + override_start_partition_arg
        return final_args


def set_defaults(parser):
    """Set default values based on environment"""
    chronon_scripts_path = os.environ.get("CHRONON_SCRIPTS_PATH", "/srv/chronon/scripts")
    today = datetime.today().strftime("%Y-%m-%d")
    parser.set_defaults(
        mode="backfill",
        ds=today,
        spark_submit_path=os.path.join(chronon_scripts_path, "spark_submit.sh"),
        conf_type="group_bys",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit various kinds of chronon jobs")
    parser.add_argument(
        "--conf",
        required=False,
        help="Conf param - required for every mode",
    )
    parser.add_argument("--mode", choices=SPARK_MODES)
    parser.add_argument("--ds", help="the end partition to backfill the data")
    parser.add_argument("--app-name", help="app name. Default to {}".format(APP_NAME_TEMPLATE))
    parser.add_argument(
        "--start-ds",
        help="override the original start partition for a range backfill. "
        "It only supports staging query, group by backfill and join jobs. "
        "It could leave holes in your final output table due to the override date range.",
    )
    parser.add_argument("--end-ds", help="the end ds for a range backfill")
    parser.add_argument("--spark-submit-path", help="Path to spark-submit")
    parser.add_argument(
        "--conf-type",
        help="Config type (group_bys, joins, staging_queries) - only needed when --conf is not provided",
    )
    set_defaults(parser)
    args, unknown_args = parser.parse_known_args()
    args.args = " ".join(unknown_args)
    set_runtime_env(args)
    if args.mode not in SPARK_MODES:
        raise ValueError(f"Invalid mode: {args.mode}. Must be one of: {SPARK_MODES}")
    Runner(args).run()
