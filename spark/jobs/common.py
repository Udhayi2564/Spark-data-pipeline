import os
import sys
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv


# Configure Windows Spark prerequisites before importing PySpark. This avoids
# stale PYSPARK_* or HADOOP_HOME values from a previous terminal session.
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
python_executable: Path = Path(sys.executable).resolve()

if not python_executable.is_file():
    raise FileNotFoundError(
        f"Python executable was not found: {python_executable}. "
        "Activate the project .venv before running Spark."
    )

os.environ["PYSPARK_PYTHON"] = str(python_executable)
os.environ["PYSPARK_DRIVER_PYTHON"] = str(python_executable)

from pyspark.sql import SparkSession


load_dotenv(PROJECT_ROOT / ".env")

JARS_DIR: Path = PROJECT_ROOT / "spark" / "jars"


ORACLE_HOST: str = os.getenv(
    "ORACLE_HOST",
    "localhost"
)

ORACLE_PORT: str = os.getenv(
    "ORACLE_PORT",
    "1521"
)

ORACLE_SERVICE: str = os.getenv(
    "ORACLE_SERVICE",
    "FREEPDB1"
)

ORACLE_USER: str = os.getenv(
    "ORACLE_USER",
    "sh"
)

ORACLE_PASSWORD: str = os.getenv(
    "ORACLE_PASSWORD",
    ""
)


POSTGRES_HOST: str = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT: str = os.getenv(
    "POSTGRES_PORT",
    "5433"
)

POSTGRES_DB: str = os.getenv(
    "POSTGRES_DB",
    "etl_target"
)

POSTGRES_USER: str = os.getenv(
    "POSTGRES_USER",
    "postgres"
)

POSTGRES_PASSWORD: str = os.getenv(
    "POSTGRES_PASSWORD",
    ""
)


APP_NAME: str = os.getenv(
    "SPARK_APP_NAME",
    "MDS_Oracle_Spark_Postgres"
)



def get_oracle_url() -> str:
    """
    Build the Oracle JDBC connection URL.
    """

    oracle_url: str = (
        f"jdbc:oracle:thin:@//"
        f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
    )

    return oracle_url



def get_postgres_url() -> str:
    """
    Build the PostgreSQL JDBC connection URL.
    """

    postgres_url: str = (
        f"jdbc:postgresql://"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    return postgres_url


def get_jar_paths() -> str:
    """
    Find all JDBC driver JAR files.
    """

    jars: list[str] = [
        str(path)
        for path in JARS_DIR.glob("*.jar")
    ]

    if not jars:
        raise FileNotFoundError(
            "No JDBC JARs found. "
            "Run scripts\\setup_drivers.ps1 first."
        )

    jar_paths: str = ";".join(jars)

    return jar_paths


def create_spark() -> SparkSession:
    """
    Create and configure the SparkSession.

    The same Python executable is explicitly used by:
        1. Spark driver
        2. Spark Python workers
    """


    python_path: str = str(python_executable)



    os.environ["PYSPARK_PYTHON"] = python_path

    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


    os.environ["TZ"] = "Asia/Kolkata"


    jar_paths: str = get_jar_paths()



    spark: SparkSession = (
        SparkSession.builder
        .appName(APP_NAME)
        .master("local[*]")


        .config(
            "spark.sql.session.timeZone",
            "Asia/Kolkata"
        )

        # Use Hadoop's Java filesystem implementation on Windows. The native
        # DLL ABI varies by Hadoop build and is unnecessary for local output.
        .config(
            "spark.hadoop.io.native.lib",
            "false"
        )

        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem"
        )


        .config(
            "spark.pyspark.python",
            python_path
        )


        .config(
            "spark.pyspark.driver.python",
            python_path
        )


        .config(
            "spark.driver.extraClassPath",
            jar_paths
        )

        .config(
            "spark.executor.extraClassPath",
            jar_paths
        )

        .getOrCreate()
    )


    spark._jvm.java.util.TimeZone.setDefault(
        spark._jvm.java.util.TimeZone.getTimeZone(
            "Asia/Kolkata"
        )
    )


    spark.sparkContext.setLogLevel("WARN")

    return spark


def oracle_properties() -> Dict[str, str]:
    """
    Return Oracle JDBC connection properties.
    """

    properties: Dict[str, str] = {
        "user": ORACLE_USER,
        "password": ORACLE_PASSWORD,
        "driver": "oracle.jdbc.OracleDriver"
    }

    return properties


def postgres_properties() -> Dict[str, str]:
    """
    Return PostgreSQL JDBC connection properties.
    """

    properties: Dict[str, str] = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    return properties


def require_credentials() -> None:
    """
    Check whether required database credentials exist.
    """

    missing: list[str] = []

    if not ORACLE_PASSWORD:
        missing.append("ORACLE_PASSWORD")

    if not POSTGRES_PASSWORD:
        missing.append("POSTGRES_PASSWORD")

    if missing:
        raise ValueError(
            "Missing environment variables: "
            + ", ".join(missing)
        )