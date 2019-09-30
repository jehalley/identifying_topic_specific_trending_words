#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 16:48:00 2019

@author: JeffHalley
"""

from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand
from app import app, db


migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)


if __name__ == '__main__':
    manager.run()