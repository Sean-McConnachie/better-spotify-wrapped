import os.path

import re
import json
from logging import Logger
from pydantic.fields import Field

from ollama import Message, Client
from typing import TypeVar, Generic, Any, Self
from pydantic import BaseModel
import requests
from bs4 import BeautifulSoup
from typing_extensions import override

from persistence import Config, load_model_json

DEBUG = False
EVERYNOISE_URL = "https://everynoise.com/everynoise1d.html"

cfg = load_model_json(Config, "config.json")

T = TypeVar("T", bound="JsonOutputFormat")


class JsonOutputFormat(BaseModel):
    """Wrapper for LLM Generations

    Attributes:
        variable_name (_type_): the variable name and type of something for the LLM to generate

    """

    @staticmethod
    def fields() -> dict[str, str]:
        """Generate a dictionary of fields for the output format.

        Raises:
            NotImplementedError: Needs to be overridden by subclass.

        Returns:
            dict[str, str]: {field_name: field_type}
        """
        raise NotImplementedError("fields method not implemented")


class JsonPrompt(BaseModel, Generic[T]):
    """Utility class for making sending LLM requests to the server easier

    Attributes:
        output_format (_T_): The format of the output, generally a json
        system_prompt (_str_): base message to add to all prompts
        msgs (_list[str]_): a list of messages to add to the prompt
    """
    output_format: T
    system_prompt: str | None = Field(default=None)
    msgs: list[Message]

    def pop_last(self):
        self.msgs.pop()

    def add_msg(self, msg: Message):
        self.msgs.append(msg)

    def add_to_last(self, content: str):
        self.msgs[-1]["content"] += content  # type: ignore

    def pop_add(self, msg: Message):
        self.msgs.pop()
        self.add_msg(msg)

    def build_prompt(self, output_format: JsonOutputFormat) -> list[Message]:
        system_msg = f"""\
Do not output any markdown. Do not make any comments. Do not use escape characters.

Output nothing but a JSON in the following format: {json.dumps(output_format.fields())}"""
        msgs = [Message(
            role="system", content=system_msg
        )] + self.msgs
        return msgs


class LLMConnector:
    """Creates a connection to an LLM server

    Parameters:
        log (_Logger_): the logger used to log
        cli (_Client_): the client used to connect to the LLM server
        model (_str_): the model to use for the LLM server
    """
    log: Logger
    cli: Client
    model: str

    def __init__(self, log: Logger, host: str, model: str):
        self.log = log
        self.cli = Client(host=host)
        self.model = model

    def json_prompt(self, prompt: JsonPrompt[T], max_tries: int = 20) -> T | None:
        """Generate a chat response from a json prompt.

        Args:
            prompt (JsonPrompt): The json prompt to send to the LLM server.

        Returns:
            T: same as return_type, but with the data from the LLM server.
        """
        if max_tries == 0:
            return None

        msgs = prompt.build_prompt(prompt.output_format)
        self.log.debug(f"Making json prompt request with {len(prompt.msgs)} messages.")
        resp = self.cli.chat(model=self.model, messages=msgs).message.content
        prompt.add_msg(Message(role="assistant", content=resp))

        # Parse the response and return the output format
        try:
            # replace \& with & recursively until no more are found (mainly because of R&B)
            while "\\&" in resp:
                resp = resp.replace("\\&", "&")
            # basic regex to extract info between { }
            # elimates markdown format errors, the most common type of format error
            resp = re.search("{[^}]*}", resp)
            if resp is None:
                raise ValueError("No JSON found in response.")
            resp = resp.group(0)
            output_format = prompt.output_format.model_validate_json(resp)
        except ValueError as e:
            self.log.error(f"Error parsing response: {e}. Retrying. Resp: {resp}")
            prompt.add_msg(Message(
                role="user", content=f"That did not work. Here is the error I get: `{e}`. Please try again."
            ))
            return self.json_prompt(prompt, max_tries - 1)

        return output_format


def fetch_everynoise():
    def make_request():
        response = requests.get(EVERYNOISE_URL)
        return response.text

    if not DEBUG:
        if os.path.exists("everynoise.html"):
            os.remove("everynoise.html")
        data = make_request()
    else:
        if not os.path.exists("everynoise.html"):
            data = make_request()
            with open("everynoise.html", "w") as f:
                f.write(data)
        else:
            with open("everynoise.html", "r") as f:
                data = f.read()

    return data


def get_all_genres(data: str) -> list[str]:
    soup = BeautifulSoup(data, "lxml")
    genres = soup.select("table > tr > td:nth-of-type(3) > a")
    return [genre.text for genre in genres]


class ClassifyGenreOutput(JsonOutputFormat):
    reason: str
    fundamental_genre: str

    @staticmethod
    def fields():
        return {
            "reason": "str",
            "fundamental_genre": "str"
        }

    @override
    def model_validate_json(
            cls,
            json_data: str | bytes | bytearray,
            *,
            strict: bool | None = None,
            context: Any | None = None,
    ) -> Self:
        s: "ClassifyGenreOutput" = super().model_validate_json(json_data, strict=strict, context=context)
        s.fundamental_genre = s.fundamental_genre.lower()
        if s.fundamental_genre not in cfg.fundamental_genres:
            raise ValueError(f"Invalid fundamental genre: {s.fundamental_genre}. Choose from {cfg.fundamental_genres}")
        return s


def classify_genres(all_genres: list[str], fundamental_genres: list[str], llm: LLMConnector) -> dict[str, str]:
    classifed = {}
    for i, genre in enumerate(all_genres):
        prompt = JsonPrompt(
            output_format=ClassifyGenreOutput.model_construct(),
            system_prompt="""\
""".format(
                fundamentals=json.dumps(fundamental_genres, indent=2)
            ),
            msgs=[Message(
                role="user",
                content="""\
You are tasked with classifying sub-genres of music into their respective fundamental genres.

Here is a list of the fundamental genres:
{fundamentals}

You must first give your reasoning for why the sub-genre belongs to the fundamental genre.

Then, you must output the exact fundamental genre that the sub-genre belongs to. Do not change case or spelling.

Classify the sub-genre: \"{genre}\"""".format(
                    fundamentals=json.dumps(fundamental_genres, indent=2),
                    genre=genre
                )
            )]
        )
        output = llm.json_prompt(prompt)
        if output is None:
            print(f" > ({i + 1}/{len(all_genres)}) {genre} -> ERROR")
            classifed[genre] = None
        else:
            print(f" > ({i + 1}/{len(all_genres)}) {genre} -> {output.fundamental_genre} ({output.reason})")
            classifed[genre] = output.fundamental_genre
    return classifed


def main(cfg: Config):
    data = fetch_everynoise()
    genres = get_all_genres(data)
    llm = LLMConnector(Logger("llm"), cfg.ollama_host, cfg.ollama_model)
    classified = classify_genres(genres, cfg.fundamental_genres, llm)
    with open(cfg.classified_genres_fp, "w") as f:
        json.dump(classified, f, indent=2)


if __name__ == "__main__":
    main(cfg)
