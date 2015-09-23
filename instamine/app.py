import mysql.connector

from triad_mine import TriadFinder, TriadChangeFinder, TriadMiner, Mine
from mimesis import MySqlTriadMimesis

miner = TriadMiner()
mine = Mine(miner)
mine.start()

triad_mimesis = MySqlTriadMimesis(mysql.connector.connect(user='instamine',
                                  password='instamine',
                                  host='localhost',
                                  database='instamine'))
finder = TriadFinder(triad_mimesis)
finder.work()

changes = TriadChangeFinder(triad_mimesis)
changes.dig_changes()

miner.effort_fin()
changes.effort_fin()
triad_mimesis.close()
