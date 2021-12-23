import boto3
import click
import os
import tarfile
import subprocess

from botocore.exceptions import NoCredentialsError
from typing import List
from datetime import datetime
from .. import bindmounts
from .. import config as tutor_config
from .. import env as tutor_env
from ..exceptions import TutorError
from .. import fmt
from .. import jobs
from ..types import Config
from .. import utils
from .context import BaseJobContext


class ComposeJobRunner(jobs.BaseComposeJobRunner):
    def __init__(self, root: str, config: Config):
        super().__init__(root, config)
        self.project_name = ""
        self.docker_compose_files: List[str] = []
        self.docker_compose_job_files: List[str] = []

    def docker_compose(self, *command: str) -> int:
        """
        Run docker-compose with the right yml files.
        """
        args = []
        for docker_compose_path in self.docker_compose_files:
            if os.path.exists(docker_compose_path):
                args += ["-f", docker_compose_path]
        return utils.docker_compose(
            *args, "--project-name", self.project_name, *command
        )

    def run_job(self, service: str, command: str) -> int:
        """
        Run the "{{ service }}-job" service from local/docker-compose.jobs.yml with the
        specified command.
        """
        run_command = []
        for docker_compose_path in self.docker_compose_job_files:
            path = tutor_env.pathjoin(self.root, docker_compose_path)
            if os.path.exists(path):
                run_command += ["-f", path]
        run_command += ["run", "--rm"]
        if not utils.is_a_tty():
            run_command += ["-T"]
        job_service_name = "{}-job".format(service)
        return self.docker_compose(
            *run_command,
            job_service_name,
            "sh",
            "-e",
            "-c",
            command,
        )


class BaseComposeContext(BaseJobContext):
    def job_runner(self, config: Config) -> ComposeJobRunner:
        raise NotImplementedError


@click.command(
    short_help="Run all or a selection of services.",
    help="Run all or a selection of services. Docker images will be rebuilt where necessary.",
)
@click.option("--skip-build", is_flag=True, help="Skip image building")
@click.option("-d", "--detach", is_flag=True, help="Start in daemon mode")
@click.argument("services", metavar="service", nargs=-1)
@click.pass_obj
def start(
    context: BaseComposeContext, skip_build: bool, detach: bool, services: List[str]
) -> None:
    command = ["up", "--remove-orphans"]
    if not skip_build:
        command.append("--build")
    if detach:
        command.append("-d")

    # Start services
    config = tutor_config.load(context.root)
    context.job_runner(config).docker_compose(*command, *services)


@click.command(help="Stop a running platform")
@click.argument("services", metavar="service", nargs=-1)
@click.pass_obj
def stop(context: BaseComposeContext, services: List[str]) -> None:
    config = tutor_config.load(context.root)
    context.job_runner(config).docker_compose("stop", *services)


@click.command(
    short_help="Reboot an existing platform",
    help="This is more than just a restart: with reboot, the platform is fully stopped before being restarted again",
)
@click.option("-d", "--detach", is_flag=True, help="Start in daemon mode")
@click.argument("services", metavar="service", nargs=-1)
@click.pass_context
def reboot(context: click.Context, detach: bool, services: List[str]) -> None:
    context.invoke(stop, services=services)
    context.invoke(start, detach=detach, services=services)


@click.command(
    short_help="Restart some components from a running platform.",
    help="""Specify 'openedx' to restart the lms, cms and workers, or 'all' to
restart all services. Note that this performs a 'docker-compose restart', so new images
may not be taken into account. It is useful for reloading settings, for instance. To
fully stop the platform, use the 'reboot' command.""",
)
@click.argument("services", metavar="service", nargs=-1)
@click.pass_obj
def restart(context: BaseComposeContext, services: List[str]) -> None:
    config = tutor_config.load(context.root)
    command = ["restart"]
    if "all" in services:
        pass
    else:
        for service in services:
            if service == "openedx":
                if config["RUN_LMS"]:
                    command += ["lms", "lms-worker"]
                if config["RUN_CMS"]:
                    command += ["cms", "cms-worker"]
            else:
                command.append(service)
    context.job_runner(config).docker_compose(*command)


@click.command(help="Initialise all applications")
@click.option("-l", "--limit", help="Limit initialisation to this service or plugin")
@click.pass_obj
def init(context: BaseComposeContext, limit: str) -> None:
    config = tutor_config.load(context.root)
    runner = context.job_runner(config)
    jobs.initialise(runner, limit_to=limit)


@click.command(help="Create an Open edX user and interactively set their password")
@click.option("--superuser", is_flag=True, help="Make superuser")
@click.option("--staff", is_flag=True, help="Make staff user")
@click.option(
    "-p",
    "--password",
    help="Specify password from the command line. If undefined, you will be prompted to input a password",
)
@click.argument("name")
@click.argument("email")
@click.pass_obj
def createuser(
    context: BaseComposeContext,
    superuser: str,
    staff: bool,
    password: str,
    name: str,
    email: str,
) -> None:
    config = tutor_config.load(context.root)
    runner = context.job_runner(config)
    command = jobs.create_user_command(superuser, staff, name, email, password=password)
    runner.run_job("lms", command)


@click.command(
    help="Assign a theme to the LMS and the CMS. To reset to the default theme , use 'default' as the theme name."
)
@click.option(
    "-d",
    "--domain",
    "domains",
    multiple=True,
    help=(
        "Limit the theme to these domain names. By default, the theme is "
        "applied to the LMS and the CMS, both in development and production mode"
    ),
)
@click.argument("theme_name")
@click.pass_obj
def settheme(context: BaseComposeContext, domains: List[str], theme_name: str) -> None:
    config = tutor_config.load(context.root)
    runner = context.job_runner(config)
    domains = domains or jobs.get_all_openedx_domains(config)
    jobs.set_theme(theme_name, domains, runner)


@click.command(help="Import the demo course")
@click.pass_obj
def importdemocourse(context: BaseComposeContext) -> None:
    config = tutor_config.load(context.root)
    runner = context.job_runner(config)
    fmt.echo_info("Importing demo course")
    jobs.import_demo_course(runner)


@click.command(
    short_help="Run a command in a new container",
    help=(
        "Run a command in a new container. This is a wrapper around `docker-compose run`. Any option or argument passed"
        " to this command will be forwarded to docker-compose. Thus, you may use `-v` or `-p` to mount volumes and"
        " expose ports."
    ),
    context_settings={"ignore_unknown_options": True},
)
@click.argument("args", nargs=-1, required=True)
@click.pass_context
def run(context: click.Context, args: List[str]) -> None:
    extra_args = ["--rm"]
    if not utils.is_a_tty():
        extra_args.append("-T")
    context.invoke(dc_command, command="run", args=[*extra_args, *args])


@click.command(
    name="bindmount",
    help="Copy the contents of a container directory to a ready-to-bind-mount host directory",
)
@click.argument(
    "service",
)
@click.argument("path")
@click.pass_obj
def bindmount_command(context: BaseComposeContext, service: str, path: str) -> None:
    config = tutor_config.load(context.root)
    host_path = bindmounts.create(context.job_runner(config), service, path)
    fmt.echo_info(
        "Bind-mount volume created at {}. You can now use it in all `local` and `dev` commands with the `--volume={}` option.".format(
            host_path, path
        )
    )


@click.command(
    short_help="Run a command in a running container",
    help=(
        "Run a command in a running container. This is a wrapper around `docker-compose exec`. Any option or argument"
        " passed to this command will be forwarded to docker-compose. Thus, you may use `-e` to manually define"
        " environment variables."
    ),
    context_settings={"ignore_unknown_options": True},
    name="exec",
)
@click.argument("args", nargs=-1, required=True)
@click.pass_context
def execute(context: click.Context, args: List[str]) -> None:
    context.invoke(dc_command, command="exec", args=args)


@click.command(
    short_help="View output from containers",
    help="View output from containers. This is a wrapper around `docker-compose logs`.",
)
@click.option("-f", "--follow", is_flag=True, help="Follow log output")
@click.option("--tail", type=int, help="Number of lines to show from each container")
@click.argument("service", nargs=-1)
@click.pass_context
def logs(context: click.Context, follow: bool, tail: bool, service: str) -> None:
    args = []
    if follow:
        args.append("--follow")
    if tail is not None:
        args += ["--tail", str(tail)]
    args += service
    context.invoke(dc_command, command="logs", args=args)


@click.command(
    short_help="Direct interface to docker-compose.",
    help=(
        "Direct interface to docker-compose. This is a wrapper around `docker-compose`. Most commands, options and"
        " arguments passed to this command will be forwarded as-is to docker-compose."
    ),
    context_settings={"ignore_unknown_options": True},
    name="dc",
)
@click.argument("command")
@click.argument("args", nargs=-1)
@click.pass_obj
def dc_command(context: BaseComposeContext, command: str, args: List[str]) -> None:
    config = tutor_config.load(context.root)
    volumes, non_volume_args = bindmounts.parse_volumes(args)
    volume_args = []
    for volume_arg in volumes:
        if ":" not in volume_arg:
            # This is a bind-mounted volume from the "volumes/" folder.
            host_bind_path = bindmounts.get_path(context.root, volume_arg)
            if not os.path.exists(host_bind_path):
                raise TutorError(
                    (
                        "Bind-mount volume directory {} does not exist. It must first be created"
                        " with the '{}' command."
                    ).format(host_bind_path, bindmount_command.name)
                )
            volume_arg = "{}:{}".format(host_bind_path, volume_arg)
        volume_args += ["--volume", volume_arg]
    context.job_runner(config).docker_compose(command, *volume_args, *non_volume_args)


@click.command(help="Backup mongodb and mysql databases.")
@click.pass_obj
def backup(context):
    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    config = tutor_config.load(context.root)
    runner = context.job_runner(config)

    mongodb_command = ["exec", "mongodb", "mongodump", "--out=/data/db/dump_{}.mongodb".format(now)]
    runner.docker_compose(*mongodb_command)
    fmt.echo_info("Exported mongo databases successfully")

    mysql_root_pwd = config['MYSQL_ROOT_PASSWORD']
    mysql_command = ["exec", "mysql"]
    mysql_command += [
        "mysqldump", 
        "--all-databases", 
        "-u", "root", 
        "--password={}".format(mysql_root_pwd), 
        "--result-file=/var/lib/mysql/dump_{}.sql".format(now)
    ]
    runner.docker_compose(*mysql_command)
    fmt.echo_info("Exported mysql databases successfully")

    mongodb_dump_path = tutor_env.data_path(
        context.root, 'mongodb/dump_{}.mongodb'.format(now))
    mysql_dump_path = tutor_env.data_path(
        context.root, 'mysql/dump_{}.sql'.format(now))
    dirFiles = [
        mongodb_dump_path,
        mysql_dump_path,
        tutor_env.data_path(context.root, 'cms'),
        tutor_env.data_path(context.root, 'lms'),
        tutor_env.data_path(context.root, 'openedx-media'),
    ]

    root_dir = tutor_env.root_dir(context.root)
    dirFiles.append(root_dir + '/config.yml')
    file_name = '{}_{}.tar.gz'.format(config['LMS_HOST'], now)
    backup_path = tutor_env.data_path(context.root, 'backup', file_name)

    utils.ensure_file_directory_exists(backup_path)

    with tarfile.open(backup_path, 'w:gz') as tar:
        for dir in dirFiles:
            tar.add(dir, arcname=os.path.basename(dir))

        fmt.echo_info("Created backup file {} successfully".format(file_name))

        try:
            subprocess.run(["sudo", "rm", "-rf", mongodb_dump_path])
            if os.path.exists(mysql_dump_path):
                os.remove(mysql_dump_path)
        except OSError as e:
            fmt.echo_info("Delete mongodb or mysql dump files {} are failed".format(e.strerror))
        
        if "BACKUP_ENABLED" in config:
            if config["BACKUP_ENABLED"]:
                s3_access_key = config["BACKUP_S3_ACCESS_KEY"]
                s3_secret_key = config["BACKUP_S3_SECRET_KEY"]
                s3_bucket_name = config["BACKUP_S3_BUCKET_NAME"]

                s3 = boto3.client(
                    's3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

                try:
                    s3.upload_file(backup_path, s3_bucket_name, file_name)
                    fmt.echo_info(
                        "Uploaded backup file {} to S3 successfully".format(file_name))
                    
                    if os.path.exists(backup_path):
                        os.remove(backup_path)

                except FileNotFoundError:
                    fmt.echo_info(
                        "The backup file {} was not found".format(file_name))
                except NoCredentialsError:
                    fmt.echo_info("The S3 credential is not available")


def add_commands(command_group: click.Group) -> None:
    command_group.add_command(start)
    command_group.add_command(stop)
    command_group.add_command(restart)
    command_group.add_command(reboot)
    command_group.add_command(init)
    command_group.add_command(createuser)
    command_group.add_command(importdemocourse)
    command_group.add_command(settheme)
    command_group.add_command(dc_command)
    command_group.add_command(run)
    command_group.add_command(bindmount_command)
    command_group.add_command(execute)
    command_group.add_command(logs)
    command_group.add_command(backup)
