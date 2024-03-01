import json
import logging
import traceback
from argparse import ArgumentParser
from concurrent.futures import Future  # noqa

from vectordb_bench import config
from vectordb_bench.backend.cases import CaseType
from vectordb_bench.backend.clients.api import DBConfig, EmptyDBCaseConfig
from vectordb_bench.interface import BenchMarkRunner
from vectordb_bench.models import DB, CaseConfig, TaskConfig

log = logging.getLogger(__name__)


class CMDRun:
    def __init__(self, args):
        self.db: DB = DB[args.database]
        self.db_config: DBConfig = self.db.config_cls(**json.loads(args.db_config))
        self.cases: CaseType = [CaseType[case] for case in args.cases.split(",")]
        self.label = args.label

    def run_from_cmd(self):
        try:
            task_configs = []
            for case in self.cases:
                task_config = TaskConfig(
                    db=self.db,
                    db_config=self.db_config,
                    db_case_config=EmptyDBCaseConfig(),
                    case_config=CaseConfig(case_id=case),
                )
                task_configs.append(task_config)

            runner = BenchMarkRunner()
            runner.run(task_configs, task_label=self.label)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            log.warning(
                f"exit, err={e}\nstack trace={traceback.format_exc(chain=True)}"
            )


def get_args():
    parser = ArgumentParser()

    parser.add_argument(
        "-c", "--db-config", dest="db_config", default=None, help="Db config"
    )
    parser.add_argument(
        "-d", "--database", default=None, help="Database name as listed in DB enum"
    )
    parser.add_argument("-t", "--cases", default="", help="Cases separated by comma")
    parser.add_argument("-l", "--label", default="", help="label")

    return parser.parse_args()


def main():
    args = get_args()
    log.info(f"all configs: {config().display()}")
    CMDRun(args).run_from_cmd()


if __name__ == "__main__":
    main()
