#!/usr/bin/env python3

import sys
import datetime
from time import time, mktime
from fastavro import writer, reader, parse_schema
import os.path as path
import json
from pprint import pprint


def t(*dt):
    return int(mktime(datetime.datetime(*dt).timetuple()))


ORDERS = [
    {
        "codigo_pedido": 1, "data_pedido": t(2019, 10, 2),
        "tipo_entrega": "FISICO", "email_cliente": None,
        "endereco_entrega": {
            "logradouro": "Rua Jamil Macedo, 58",
            "bairro": "Jardim Gramado",
            "cidade": "Jurupema",
            "estado": "GO",
            "CEP": "75212-200",
            "telefone": "(62) 93295-2201"
        },
        "produtos": [
            {"codigo": 1, "nome": "Liquidificador", "preco": "150,99",
                "descricao": "Belo liquidificador"},
            {"codigo": 2, "nome": "Refrigerador",
                "preco": "1399,99", "descricao": None}
        ]
    },
    {
        "codigo_pedido": 1, "data_pedido": t(2019, 9, 15),
        "tipo_entrega": "DIGITAL", "email_cliente": "cliente1@pedido.com", "endereco_entrega": None,
        "produtos": [
            {"codigo": 1, "nome": "The Amazing Spiderman",
                "preco": "59,99", "descricao": None},
            {"codigo": 2, "nome": "Assassin's Creed Odissey",
                "preco": "199,99", "descricao": None}
        ]
    },
]
ORDERS = ORDERS * 10_000


def schema_path(schema_name):
    return path.join(path.dirname(__file__),
                     '..', 'schema', f'{schema_name}.avsc')


def read_orders(in_filename):
    counter = 0
    sample = None
    t0 = time()
    with open(in_filename, 'rb') as f:
        avro_reader = reader(f)
        for pedido in avro_reader:
            if counter == 0:
                print("Primeira iteracao em {:0.8f}s".format(time() - t0))
                sample = pedido
            counter += 1
    delta = time() - t0
    print("{} registros lidos em {:0.3f}s".format(counter, delta))
    print("Exemplo de registro:")
    pprint(sample)


def write_orders(out_filename):
    with open(schema_path('pedido'), 'r') as f:
        schema = json.load(f)
    schema = parse_schema(schema)
    t0 = time()
    with open(out_filename, 'wb') as out:
        writer(out, schema, ORDERS, codec='deflate')
    delta = time() - t0
    print("{} registros escritos em {:0.3f}s".format(len(ORDERS), delta))
    with open(out_filename.replace(".avro", ".json"), "w") as f:
        json.dump(ORDERS, f, separators=(',', ':'))


def main():
    arg = sys.argv[1]
    filename = sys.argv[2]
    if arg == "write":
        write_orders(filename)
    elif arg == "read":
        read_orders(filename)


if __name__ == "__main__":
    main()
