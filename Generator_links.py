import json


def Generate_links(file_name):
    list_links = []
    list_checker_links = []

    with open(file_name, "r", encoding="utf-8") as file:
        links = file.read().strip().split("\n")[1:]

    for i in range(len(links)):
        list_links.append({"url": links[i], "id": i+1})

    with open("links.json", "w") as file:
        json.dump(list_links, file, indent=4)
