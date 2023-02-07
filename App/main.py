import os,sys
import glob
import pandas as pd
import numpy as np
import datetime
import scipy
import tkinter
import subprocess
import sqlalchemy
import pyodbc

with open('Previsor.py', 'r', encoding = 'utf-8') as f:
    lines = "".join(f.readlines())

exec(lines)