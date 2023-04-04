# parsivel2file.py
A pyserial derived class for sampling a parsivel disdrometer from ott via python
This program is loosely based on the [parsivel serial tools](https://github.com/lacros-tropos/parsivel2tools) form TROPOS in so far that it creates the same netCDF files (which in turn are compatible with [cloudnet](cloudnet.fmi.fi)
As such, this repo is underdocummented :-)


## Usage
Several options are available:
### 1. Standalone sampling (default)
The default is for the file to be sampling for 15 minutes on /dev/ttyUSB0 via the CS/PA pollcmd which returns all data. This is written to self.data based on the code (see manual, spectra is code 93 for example). The data is then written out to a netCDF and a ASDO-like file (with only 1 header instead of one for every record). By default, files are written out into a subdirectory structure of Y/M/D/x.nc|x.csv.

### 2. Interactive sampling (interactive/development)
Send a specific code via or simply get one sample by calling `getparsiveldata()`
Send a specific code or simply get one sample by calling ``getparsiveldata()

### 3. Commands list
#### communication / sampling - related
- `pollcode` => Sends a single code to the parsivel, which reports the measurement of that code. See parsivel manual for codes
- `help` => Returns the parsivel help (which lists CS/X commands that could be issued to the parsivel. See parsivel manual for more information
- `getconfig` => Returns the current config of the parsivel. See parsivel manual for more information
- `getparsiveldata` => Polls the parsivel with CS/PA and save the return values to self.buffer / self.data (the first being a byte string the latter being a dict which contains the answer per code)
- `write2file` => shorthand for calling `write2nc` / `write2asdofile` where each writes out data to a dailyfile in the corresponding format
- Various helper functions, such as `poll`, `clearbuffer`, `cleardata`, `clear`, `velocity_classes`, `diameter_classes`, `_setupncfile`
- More methods/attrs => See [pyserial documentation](https://pyserial.readthedocs.io/en/latest/pyserial_api.html) as the class parsivel class inherits all attrs/methods

#### time/date - related

- `settime` => sets the parsivel time to the computer time in UTC, no args
- `gettime` => gets the parsivel time, no args

- `setdate` => sets the parsivel time to the computer date in UTC, no args
- `getdate` => gets the parsivel time, no args

- `setdatetime` => sets the parsivel date and time to the computer date and time in UTC, no args
- `getdatetime` => gets the parsivel date and time

- `setrtc` => sets the parsivel rtc time to the computer datetime in UTC, no args
- `getrtc` => gets the parsivel rtc time, no args

#### location related
- `setstationname` => Sets the string passed as argument as station name
- `getstationname` => Gets the currently saved station name from the parsivel, no args

#### location related
- `setstationname` => Sets the string passed as argument as station name
- `getstationname` => Gets the currently saved station name from the parsivel, no args

## Known bugs
- Writeoutfreq should allow for writing out a new record only every x seconds but due the shape missmatch to the netCDf this is not working as intended (yet)

## Background
### Parsivel
The Parsivel is a disdrometer, measuring primarily the size / fall velocity of particles falling through a laser measurement path.
You can find more information [here](https://www.ott.com/products/meteorological-sensors-26/ott-parsivel2-laser-weather-sensor-2392/) 
The device is usually sampled via RS-485 2W, usually via a data logger or via ASDO (Windows programm). Alternatively, here a linux system was equipped with a moxa serial to usb converter (which by default resides at /dev/ttyUSB0).

### Dependencies
Hardware:
- Either a direct serial RS-485 2W connection on the computer or a converter that is available to pyserial
- Power for the Parsivel itself

Software:
- Python: netCDF4, pyserial

### Installation
- (Ensure converter is set to RS-485 2W, e.g. `setserial /dev/ttyUSB0 port 1`
- If you are using a moxa converter on linux, check out [their repo](https://github.com/Moxa-Linux/mxu11x0)
- Simply run file, either via `python3 parsivel2file.py` or change permissions (`chmod u+x parsivel2file.py`) and run directly `./parsivel2file.py`
