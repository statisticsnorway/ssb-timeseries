Set Up for Production
=====================

The main topics:

.. toctree::
   :name: basics
   :maxdepth: 1

   Configure Repositories <../configure-io>
   Configure Logging <../configure-logging>

The most basic features are reading and writing data from one or more `repositories`.
The repositories may be just different places where timeseries are stored,
but may also reflect a number of technical differences.
The I/O configuration controls the mechanisms through which it happens.
This setup also allows specifying a custom I/O handler module.
Usage of the library is not limited by the built in I/O modules.

Logging can be set up for the timeseries library,
or left to the application layer calling it.
Logging is not only an essential part of monitoring,
but also an interface to process automation.
