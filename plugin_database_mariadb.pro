#-----------------------------------------------------------------------------------------------------------------------------------
#
# PROJECT:            Engineering Workshop Tracker (engineeringShop)
# FILE:								plugin_database_mariadb.pro
# SUBSYSTEM:          Project File - MariaDB connector
# LANGUAGE:						C++
# TARGET OS:          LINUX
# LIBRARY DEPENDANCE:	None.
# NAMESPACE:          N/A
# AUTHOR:							Gavin Blakeman.
# LICENSE:            GPLv2
#
#                     Copyright 2022 Gavin Blakeman.
#
# OVERVIEW:						Project file for compiling the project
#
# HISTORY:            2022-07-05/GGB - File Created
#
#-----------------------------------------------------------------------------------------------------------------------------------

TARGET = plugin_mariadb

DESTDIR = ../engineeringShop/plugins/database

TEMPLATE = lib

QT += core
QT -= gui

CONFIG += cmdline
CONFIG += plugin no_plugin_name_prefix
CONFIG -= app_bundle
CONFIG += object_parallel_to_source

QMAKE_CXXFLAGS += -std=c++20
QMAKE_LFLAGS += -rdynamic
DEFINES += BOOST_THREAD_USE_LIB QT_GUI_LIB QT_CORE_LIB

#OBJECTS_DIR = "objects"

# The following define makes your compiler emit warnings if you use
# any feature of Qt which as been marked deprecated (the exact warnings
# depend on your compiler). Please consult the documentation of the
# deprecated API in order to know how to port your code away from it.
DEFINES += QT_DEPRECATED_WARNINGS

# You can also make your code fail to compile if you use deprecated APIs.
# In order to do so, uncomment the following line.
# You can also select to disable deprecated APIs only up to a certain version of Qt.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

INCLUDEPATH +=  \
    "../engineeringShop" \
    "../GCL" \
    "../MCL" \
    "../PCL" \
    "../SCL" \
    "/usr/local/lib" \
    "../WtExtensions"

SOURCES += \
  source/database_mariadb.cpp \
  source/plugin_database_mariadb.cpp


HEADERS += \
  include/database_mariadb.h

LIBS += -L../GCL -lGCL
LIBS += -lmysqlclient

