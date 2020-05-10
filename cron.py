#!/usr/local/bin/python
# -*- coding: utf-8 -*-
from subprocess import Popen
import threading


def main():
    p = Popen("start.bat", cwd=r".")
    stdout, stderr = p.communicate()


def printit():
  # executa a cada 4 horas
  threading.Timer(24000, printit).start()
  main()


if __name__ == '__main__':
    printit()
