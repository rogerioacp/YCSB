#!/bin/bash

createdb test
psql -f setup.sql test
#psql -f tinserts.sql teste
#psql -f inserts.sql teste
#psql -f create_ost_ftw.sql teste


