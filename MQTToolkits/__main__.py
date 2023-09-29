import os
import threading
import uuid
from time import sleep
from typing import Any, List

import typer
from click_shell import make_click_shell
from paho.mqtt.client import Client
from typer import Context
from typing_extensions import Annotated


class Publisher:
    def __init__(self, name: str, topic: str, mqtt_client: Client) -> None:
        self._name = name
        self._topic = topic
        self._mqtt_client = mqtt_client
        self._stop = False

    def publish(self, payload: Any, period: int | float):
        while not self._stop:
            sleep(period)
            result = self.mqtt_client.publish(self.topic, payload)
        typer.echo(f"Publisher {self.name} stopped.")

    @property
    def name(self):
        return self._name

    @property
    def topic(self):
        return self._topic

    @property
    def mqtt_client(self):
        return self._mqtt_client


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
        """Connect to the MQTT broker."""
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client_id = client_id or str(uuid.uuid4().int)
        self._client = Client(self.client_id)
        self.client.username_pw_set(self.username, self.password)
        try:
            self.client.connect(host, port)
        except Exception as e:
            typer.echo(e)
            raise Exception("Failed to connect to the MQTT broker.")

    def createPublisher(
        self,
        name: str | None,
        topic: str,
        payload: Any,
        period: int | float,
    ):
        """Create a publisher."""

        publisher = Publisher(name or str(uuid.uuid4().int), topic, self.client)
        self.publishers.append(publisher)
        publisher_thread = threading.Thread(
            target=publisher.publish, args=(payload, period), daemon=True
        )
        publisher_thread.start()

    def deletePublisher(self, name: str):
        """Delete a publisher thread."""
        for publisher in self.publishers:
            if publisher.name == name:
                publisher._stop = True
                self.publishers.remove(publisher)
                break

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def client_id(self):
        return self._client_id

    @property
    def client(self):
        return self._client

    @property
    def publishers(self):
        return self._publishers


app = typer.Typer()
publisher = typer.Typer()
app.add_typer(publisher, name="publisher")
toolkit = Toolkit()


@publisher.command("create")
def create(
    name: Annotated[str, typer.Option(help="The name of this publish thread.")],
    topic: Annotated[str, typer.Option(help="The name of the topic.")],
    payload: Annotated[Any, typer.Option(help="The payload to be published.")],
    period: Annotated[
        float, typer.Option(help="The period for publishing the payload.")
    ] = 1,
):
    """Create a publisher thread."""
    toolkit.createPublisher(name, topic, payload, period)


@publisher.command("list")
def list():
    """List all the publisher threads."""
    for publisher in toolkit.publishers:
        typer.echo(publisher.name)


@publisher.command("delete")
def delete(
    name: Annotated[str, typer.Argument(help="The name of the publisher thread.")]
):
    """Delete a publisher thread."""
    toolkit.deletePublisher(name)


@app.callback(invoke_without_command=True)
def launch(ctx: Context):
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
        password = typer.prompt("MQTT Password")
        os.environ["MQTT_PASSWORD"] = password

    try:
        toolkit.connect(host, int(port), username, password)
    except Exception as e:
        typer.echo(e)
        raise typer.Exit(1)

    shell = make_click_shell(ctx, prompt="MQTT Injector>")
    shell.cmdloop()


if __name__ == "__main__":
    app()