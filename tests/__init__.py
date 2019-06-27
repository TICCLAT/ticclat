# -*- coding: utf-8 -*-
import os


def data_dir():
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
