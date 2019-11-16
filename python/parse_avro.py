#!/usr/bin/env python3

import sys
import os.path as path
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import datetime
from time import mktime, time
import json
from pprint import pprint


def t(*dt):
    return int(mktime(datetime.datetime(*dt).timetuple()))


PEDIDOS = [
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
] * 10_000


def write_orders(out_filename):
    schema = parse_schema('pedido')
    t0 = time()
    with DataFileWriter(open(out_filename, "wb"), DatumWriter(), schema, codec='deflate') as writer:
        for order in PEDIDOS:
            writer.append(order)
    delta = time() - t0
    print("{} registros escritos em {:0.3f}s".format(len(PEDIDOS), delta))
    with open(out_filename.replace(".avro", ".json"), "w") as f:
        json.dump(PEDIDOS, f, separators=(',', ':'))


def read_orders(in_filename):
    sample = None
    counter = 0
    t0 = time()
    reader = DataFileReader(open(in_filename, 'rb'), DatumReader())
    for pedido in reader:
        if counter == 0:
            print("Primeira iteracao em {:0.8f}s".format(time() - t0))
            sample = pedido
        counter += 1
    delta = time() - t0
    print("{} registros lidos em {:0.3f}s".format(counter, delta))
    print("Exemplo de registro:")
    pprint(sample)


def schema_path(schema_name):
    return path.join(path.dirname(__file__),
                     '..', 'schema', f'{schema_name}.avsc')


def parse_schema(schema_name):
    with open(schema_path(schema_name), "rb") as f:
        return avro.schema.Parse(f.read())


def main():
    arg = sys.argv[1]
    filename = sys.argv[2]
    if arg == "write":
        write_orders(filename)
    elif arg == "read":
        read_orders(filename)


if __name__ == "__main__":
    main()
