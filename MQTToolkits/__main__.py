import enum
import os
import threading
import uuid
from datetime import datetime
from time import sleep
from typing import List

import typer
from click_shell import make_click_shell
from paho.mqtt.client import Client
from rich import print
from rich.console import Console
from rich.table import Table
from typer import Context
from typing_extensions import Annotated


class ContentType(str, enum.Enum):
    """Content type."""

    FILE = "file"
    STRING = "string"


from typing import Any
from datetime import datetime
from paho.mqtt.client import Client

class Publisher:
    """
    A class representing a MQTT publisher.

    Attributes:
    -----------
    name : str
        The name of the publisher.
    topic : str
        The topic to which the publisher publishes messages.
    mqtt_client : Client
        The MQTT client used by the publisher.
    _stop : bool
        A flag indicating whether the publisher should stop publishing messages.
    _created_at : datetime
        The datetime when the publisher was created.
    _published_messages : int
        The number of messages published by the publisher.

    Methods:
    --------
    publish(payload: str) -> None
        Publishes a message to the topic.
    """

    def __init__(self, name: str, topic: str, mqtt_client: Client) -> None:
        """
        Initializes a new instance of the Publisher class.

        Parameters:
        -----------
        name : str
            The name of the publisher.
        topic : str
            The topic to which the publisher publishes messages.
        mqtt_client : Client
            The MQTT client used by the publisher.
        """
        self._name = name
        self._topic = topic
        self._mqtt_client = mqtt_client
        self._stop = False
        self._created_at = datetime.now()
        self._published_messages = 0

    def publish(self, payload: str) -> None:
        """
        Publishes a message to the topic.

        Parameters:
        -----------
        payload : str
            The message to publish.
        """
        self._mqtt_client.publish(self.topic, payload)
        self._published_messages += 1

    @property
    def name(self) -> str:
        """
        Gets the name of the publisher.
        """
        return self._name

    @property
    def topic(self) -> str:
        """
        Gets the topic to which the publisher publishes messages.
        """
        return self._topic

    @property
    def mqtt_client(self) -> Client:
        """
        Gets the MQTT client used by the publisher.
        """
        return self._mqtt_client

    @property
    def lifetime(self) -> Any:
        """
        Gets the lifetime of the publisher.
        """
        return datetime.now() - self._created_at

    @property
    def published_messages(self) -> int:
        """
        Gets the number of messages published by the publisher.
        """
        return self._published_messages

class Toolkit:
    def __init__(self) -> None:
        self._host = str()
        self._port = int()
        self._username = str()
        self._password = str()
        self._client_id = str()
        self._client: Client = None  # type: ignore
        self._publishers: List[Publisher] = []

    def connect(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        client_id: str | None = None,
    ):
        """Connect to the MQTT broker.

        Args:
            host (str): The hostname or IP address of the MQTT broker.
            port (int): The port number of the MQTT broker.
            username (str): The username for the MQTT broker.
            password (str): The password for the MQTT broker.
            client_id (str, optional): The client ID for the MQTT broker. Defaults to None.
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client_id = client_id or str(uuid.uuid4().int)
        self._client = Client(self.client_id, reconnect_on_failure=False)
        self.client.username_pw_set(self.username, self.password)
        try:
            self.client.connect(host, port)
        except Exception as e:
            raise Exception(f"{e}\nFailed to connect to the MQTT broker.")

    def createPublisher(self, name: str | None, topic: str):
        """Create a publisher.

        Args:
            name (str, optional): The name of the publisher. Defaults to None.
            topic (str): The topic to publish to.

        Raises:
            Exception: If the publisher name already exists.
        """
        publisher = Publisher(name or str(uuid.uuid4().int), topic, self.client)
        if publisher.name in [p.name for p in self.publishers]:
            raise Exception("The publisher name already exists.")
        self.publishers.append(publisher)
        return publisher

    def deletePublisher(self, name: str):
        """Delete a publisher thread.

        Args:
            name (str): The name of the publisher to delete.
        """
        for publisher in self.publishers:
            if publisher.name == name:
                publisher._stop = True
                self.publishers.remove(publisher)
                break

    @property
    def host(self):
        """The hostname or IP address of the MQTT broker."""
        return self._host

    @property
    def port(self):
        """The port number of the MQTT broker."""
        return self._port

    @property
    def username(self):
        """The username for the MQTT broker."""
        return self._username

    @property
    def password(self):
        """The password for the MQTT broker."""
        return self._password

    @property
    def client_id(self):
        """The client ID for the MQTT broker."""
        return self._client_id

    @property
    def client(self):
        """The MQTT client instance."""
        return self._client

    @property
    def publishers(self):
        """The list of publishers."""
        return self._publishers


app = typer.Typer()
publisher = typer.Typer()
app.add_typer(publisher, name="publisher")
toolkit = Toolkit()


@publisher.command("create")
def create(
    name: Annotated[str, typer.Argument(help="The name of this publish thread.")],
    topic: Annotated[str, typer.Argument(help="The name of the topic.")],
    payload: Annotated[str, typer.Argument(help="The payload to be published.")],
    period: Annotated[
        float, typer.Argument(help="The period for publishing the payload.")
    ] = 1,
    content_type: Annotated[
        ContentType, typer.Option(help="The content type of the payload")
    ] = ContentType.STRING,
):
    """
    Create a publisher that continuously publishes the payload to the topic with the given period.

    Args:
        name (str): The name of this publish thread.
        topic (str): The name of the topic.
        payload (str): The payload to be published.
        period (float, optional): The period for publishing the payload. Defaults to 1.
        content_type (ContentType, optional): The content type of the payload. Defaults to ContentType.STRING.
    """
    try:
        publisher = toolkit.createPublisher(name, topic)
    except Exception as e:
        print(f"[bold red] {e} [/bold red]")
        return

    if content_type == ContentType.FILE:

        def _publishing():
            for line in open(payload, "r").readlines():
                if publisher._stop:
                    print(
                        f"[bold yellow] Publisher {publisher.name} interrupted. [/bold yellow]"
                    )
                    break
                sleep(period)
                publisher.publish(line)
            print(f"[bold green] Publisher {publisher.name} finished. [/bold green]")
            toolkit.publishers.remove(publisher)

        publisher_thread = threading.Thread(target=_publishing, daemon=True)
        publisher_thread.start()

        return

    if content_type == ContentType.STRING:

        def _publishing():
            while True:
                if publisher._stop:
                    break
                sleep(period)
                publisher.publish(payload)
            print(f"[bold yellow] Publisher {publisher.name} stopped. [/bold yellow]")

        publisher_thread = threading.Thread(target=_publishing, daemon=True)
        publisher_thread.start()
        return


@publisher.command("list")
def list():
    """
    List all the publishers.

    This function prints a table of all the publishers in the toolkit, including their name, topic, lifetime, and number of sent messages.
    """
    table = Table("Name", "Topic", "Life", "Sent Messages", title="Publishers")
    console = Console()
    for publisher in toolkit.publishers:
        table.add_row(
            publisher.name,
            publisher.topic,
            str(publisher.lifetime.seconds),
            str(publisher.published_messages),
        )
    console.print(table)


@publisher.command("delete")
def delete(
    name: Annotated[str, typer.Argument(help="The name of the publisher thread.")]
):
    """
    Delete a publisher.

    This function deletes a publisher with the given name. The publisher will stop publishing the payload.

    Args:
        name (str): The name of the publisher thread.

    Returns:
        None
    """
    toolkit.deletePublisher(name)


@app.callback(invoke_without_command=True)
def launch(ctx: Context):
    """
    Launches the MQTT Injector tool by connecting to the MQTT broker and starting the command line interface.

    Args:
        ctx (Context): The context object for the CLI.

    Returns:
        None
    """
    while True:
        host = os.getenv("MQTT_HOST")
        if host is None:
            host = typer.prompt("MQTT Host")
            os.environ["MQTT_HOST"] = host

        port = os.getenv("MQTT_PORT")
        if port is None:
            port = typer.prompt("MQTT Port")
            os.environ["MQTT_PORT"] = port

        username = os.getenv("MQTT_USERNAME")
        if username is None:
            username = typer.prompt("MQTT Username")
            os.environ["MQTT_USERNAME"] = username

        password = os.getenv("MQTT_PASSWORD")
        if password is None:
            password = typer.prompt("MQTT Password", hide_input=True, confirmation_prompt=True)
            os.environ["MQTT_PASSWORD"] = password

        client_id = os.getenv("MQTT_CLIENT_ID")
        if client_id is None:
            client_id = typer.prompt("MQTT Client ID")
            os.environ["MQTT_CLIENT_ID"] = client_id

        try:
            toolkit.connect(host, int(port), username, password, client_id)
            print(f"[bold green] Connected to MQTT Broker {host}:{port} [/bold green]")
            break
        except Exception as e:
            os.environ.pop("MQTT_HOST")
            os.environ.pop("MQTT_PORT")
            os.environ.pop("MQTT_USERNAME")
            os.environ.pop("MQTT_PASSWORD")
            os.environ.pop("MQTT_CLIENT_ID")
            print(f"[bold red] {e} [/bold red]")

    shell = make_click_shell(ctx, prompt="MQTT Injector>")
    shell.cmdloop()


if __name__ == "__main__":
    app()
