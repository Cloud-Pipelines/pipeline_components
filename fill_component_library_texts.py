#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "cloud-pipelines",
#   "PyYAML>=6",
#   "requests",
#   "typer",
# ]
# ///

import hashlib
import requests
import typer
import yaml


url_to_text: dict[str, str] = {}


def calculate_digest_for_component_text(text: str) -> str:
    data = text.encode("utf-8")
    data = data.replace(b"\r\n", b"\n")  # Normalizing line endings
    digest = hashlib.sha256(data).hexdigest()
    return digest


def extract_component_text_from_folder(folder: dict):
    for child_folder in folder.get("folders", []):
        extract_component_text_from_folder(child_folder)
    for component_ref in folder.get("components", []):
        component_text: str = component_ref.get("text")
        component_url: str = component_ref.get("url")
        if component_url and component_text:
            url_to_text[component_url] = component_text


def fill_component_text_in_folder(folder: dict):
    for child_folder in folder.get("folders", []):
        fill_component_text_in_folder(child_folder)
    for component_ref in folder.get("components", []):
        fill_component_text_in_component_ref(component_ref)


def fill_component_text_in_component_ref(component_ref: dict):
    url = component_ref["url"]
    print(url)
    component_text = url_to_text.get(url)
    if not component_text:
        response = requests.get(url)
        response.raise_for_status()
        component_text = response.text
        url_to_text[url] = component_text
    component_ref["digest"] = calculate_digest_for_component_text(component_text)
    component_ref["text"] = component_text
    # TODO: Add hash digest


def main(
    library_input_path: str = "./pipeline_component_library.yaml",
    full_library_output_path: str = "./pipeline_component_library.with_texts.yaml",
):

    try:
        with open(full_library_output_path, "r") as reader:
            library_dict = yaml.safe_load(reader)
            extract_component_text_from_folder(folder=library_dict)
    except:
        pass

    with open(library_input_path, "r") as reader:
        library_dict = yaml.safe_load(reader)

    fill_component_text_in_folder(library_dict)

    with open(full_library_output_path, "w") as writer:
        from cloud_pipelines._components.components import _yaml_utils

        # yaml.safe_dump(library_dict, writer)
        s = _yaml_utils.dump_yaml(library_dict)
        writer.write(s)


if __name__ == "__main__":
    typer.run(main)
