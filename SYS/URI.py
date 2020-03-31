import os, pathlib
from pathlib import Path

__dir_path = os.path.dirname(os.path.realpath(__file__))
project_path = proj_path = Path(__dir_path).parent

data_dir = pathlib.Path(project_path).joinpath("./data")
raw_data = pathlib.Path(data_dir).joinpath("./raw-data")
cln_data = pathlib.Path(data_dir).joinpath("./raw-clean")

tag_data = pathlib.Path(data_dir).joinpath("./analysis-tag")
sen_data = pathlib.Path(data_dir).joinpath("./analysis-sentiment")
tok_data = pathlib.Path(data_dir).joinpath("./analysis-tokens")
ner_data = pathlib.Path(data_dir).joinpath("./analysis-ner")

sen_resu = pathlib.Path(data_dir).joinpath("./result-sentiment")
hea_resu = pathlib.Path(data_dir).joinpath("./result-heat")
ner_resu = pathlib.Path(data_dir).joinpath("./result-ner")
tok_resu = pathlib.Path(data_dir).joinpath("./result-token")
tag_resu = pathlib.Path(data_dir).joinpath("./result-tag")

show_supp_true = str(pathlib.Path(sen_resu).joinpath("./True.parquet.gzip/*"))
show_supp_false = str(pathlib.Path(sen_resu).joinpath("./False.parquet.gzip/*"))

show_senti_trump_true = str(pathlib.Path(sen_data).joinpath("./True-Trump.parquet.gzip/*"))
show_senti_hilla_true = str(pathlib.Path(sen_data).joinpath("./True-Hillary.parquet.gzip/*"))

show_heat_trump_true   = str(pathlib.Path(hea_resu).joinpath("./True-Trump.parquet.gzip/*"))
print(show_heat_trump_true)
show_heat_hillary_true = str(pathlib.Path(hea_resu).joinpath("./True-Hillary.parquet.gzip/*"))
print(show_heat_hillary_true)
show_token_true = str(pathlib.Path(tok_resu).joinpath("./*"))
show_ner_true   = str(pathlib.Path(ner_resu).joinpath("./*"))


print(proj_path)