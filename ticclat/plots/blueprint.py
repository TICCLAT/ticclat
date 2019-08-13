from flask import Blueprint, jsonify, request
from bokeh.embed import json_item

from ticclat.plots.corpus_size import corpus_size
from ticclat.plots.lexicon_size import lexicon_size
from ticclat.plots.paradigm_size import paradigm_size
from ticclat.plots.word_count_per_year import word_count_per_year

plots = Blueprint('plots', __name__)


@plots.route("/word_count_per_year")
def _word_count_per_year():
    return jsonify(json_item(word_count_per_year()))


@plots.route("/corpus_size")
def _corpus_size():
    return jsonify(json_item(corpus_size()))


@plots.route("/lexicon_size")
def _lexicon_size():
    return jsonify(json_item(lexicon_size()))


@plots.route("/paradigm_size")
def _paradigm_size():
    return jsonify(json_item(paradigm_size(request.args.get('var', 'X'))))
