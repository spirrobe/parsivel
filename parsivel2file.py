#!/bin/python3
import os
import time

import datetime
import serial

import numpy as np
import netCDF4 as nc

class parsivel_moxa(serial.Serial):
    def __init__(self,
                 # serial port parameters
                 # moxa is created as virtual USB0
                 # to setup most likely the RS-485 2W has to be activated
                 # by running
                 #setserial /dev/ttyUSB0 port 1
                 # for this to work, and also this program, the user than runs
                 # this user needs to be in the dialout group for fedora, which
                 # can be done via the command:
                 # sudo usermod -a -G dialout $USER
                 # BE AWARE, a logout is required
                 port='/dev/ttyUSB0',
                 baudrate = 57600,
                 ncmeta={'Station_Name': 'Eriswil (Kt. Bern, Switzerland',
                         'latitude': 47.07051,
                         'longitude': 7.87254,
                         'altitude': 921,
                         'Sensor_ID': 411994,
                         'Title': 'CLOUDLAB (cloudlab.ethz.ch) disdrometer data from OTT Parsivel-2',
                         'Institution': 'ETH Zurich',
                         'Contact': "Jan Henneberger, janhe@ethz.ch;\n \
                                     Robert Spirig, rspirig@ethz.ch;\n \
                                     Fabiola Ramelli, ramellif@ethz.ch",
                         "Author": 'Robert Spirig, rspirig@ethz.ch',
                 },
                 outpath='./',
                 stationname='Eriswil',
                 quiet=True,
                 ):

        # inherit init from serial and open the port
        super().__init__(port=port, baudrate=baudrate)
        self.quiet = quiet
        # a bytebuffer to hold the answer from the parsivel
        self.buffer = b''
        # how to decode the bytes to a sensible string, generally UTF8 is prefered
        self.codec = 'utf-8'
        # what to ask the parsivel, PA is the easiest, even if it is more than required
        self.pollcmd = b'CS/PA\r'
        # usual pollcmd for user telegram would be CS/P\r
        #pollcmd = b"CS/P\r"

        # for automatic polling, the time resolution in seconds
        self.samplinginterval = 10
        self.maxsampling = 60 * 15 * self.samplinginterval
        # where to store the data
        self.outpath = outpath
        # the prefix for the file to be used
        self.fileprefix = 'parsivel_'
        self.stationname = stationname[:10]
        # holder for current file, will be filled by subroutines
        self.ncfile = ''
        self.csvfile = ''
        # holder for all written files, will be filled by subroutines
        self.csvfiles = []
        self.ncfiles = []
        # dict to hold data order by variable
        self.data = {'-1': []}
        # to keep track of whether we expect data in the buffer
        self.polled = False
        # for waiting a tenth of a second for new bytes in the buffer
        self.waitdt = 0.1
        # to keep track of the waiting time
        self.waittime = 0
        # the upper limit of waiting
        self.maxwait = 3
        # increment buffersize to hold more than one record, maybe useless
        self.ReadBufferSize = 2**16;
        # default output order, ASDO compatible
        self.csvoutputorder = ['21','20', '01', '02', '03', '05', '06', '07',
                            '08', '10', '11', '12', '16', '17', '18', '34', '35', '93']
        # default output header, ASDO compatible
        self.csvheader = ['Date', 'Time', 'Intensity of precipitation (mm/h)', 'Precipitation since start (mm)', 'Weather code SYNOP WaWa',]
        self.csvheader += ['Weather code METAR/SPECI', 'Weather code NWS', 'Radar reflectivity (dBz)', 'MOR Visibility (m)', ]
        self.csvheader += ['Signal amplitude of Laserband', 'Number of detected particles', 'Temperature in sensor (°C)', ]
        self.csvheader += ['Heating current (A)', 'Sensor voltage (V)', 'Optics status', 'Kinetic Energy', 'Snow intensity (mm/h)', 'Spectrum']
        # add meta info forr ncfile
        self.ncmeta = {
                       'Source': 'OTT Parsivel-2 optical disdrometer',
                       'History': 'Data acquired with MOXA USB converter',
                       'Dependencies': 'external',
                       'Conventions': 'CF-1.6 where applicable',
                       'Comment': "Manual of the OTT Parsivel-2 can be found online" \
                                  " at https://www.ott.com",
                       "Licence": "For non-commercial use only. Any usage of the data"\
                                  " should be reported to the contact person(s).",
                     }

        self.ncmapping = {'09': 'interval',
                          '25': 'error_code',
                          '16': 'I_heating',
                          '17': 'V_sensor',
                          '18': 'state_sensor',
                          '10': 'sig_laser',
                          '01': 'rainfall_rate',
                          #'02': 'RR_total',
                          '03': 'synop_WaWa',
                          '04': 'synop_WW',
                          '07': 'radar_reflectivity',
                          '08': 'visibility',
                          '12': 'T_sensor',
                          '11': 'n_particles',
                          #'24': 'RR_accum',
                          '34': 'E_kin',
                          '90': 'number_concentration',
                          '91': 'fall_velocity',
                          '93': 'data_raw',
                         }

        self.ncscaling = {'01': 60 * 60 / 1000,
                         }

        # add any other information from ncmeta
        for key, value in ncmeta.items():
           #if key.lower() in ['name', 'location']:
           #    key = f'Station_{key}'
           self.ncmeta[key] = value

        if not self.isOpen():
            self.open()

        self.flush()

    def __del__(self):
        self.close()
        time.sleep(1)

    def settime(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        now = datetime.datetime.utcnow()
        cmd = b'CS/T/'+bytes(now.strftime('%H:%M:%S\r').encode(self.codec))
        if not self.quiet:
            print('Sending settime command ', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to settime from parsivel was ', answer)

        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def gettime(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        cmd = b'CS/T\r'
        if not self.quiet:
            print('Sending gettime command ', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to gettime from parsivel was ', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def setdate(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        now = datetime.datetime.utcnow()
        cmd = b'CS/D/'+bytes(now.strftime('%d.%m.%Y\r').encode(self.codec))
        if not self.quiet:
            print('Sending setdate command to parsivel ', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to setdate from parsivel was ', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def getdate(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        cmd = b'CS/D\r'
        if not self.quiet:
            print('Requesting date via ', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to getdate from parsivel was ', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def setrtc(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        now = datetime.datetime.utcnow()
        cmd = b'CS/U/'+bytes(now.strftime('%d.%m.%Y %H:%M:%S\r').encode(self.codec))
        if not self.quiet:
            print('Sending setrtc command', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to setrtc from parsivel was ', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def getrtc(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        cmd = b'CS/U\r'
        if not self.quiet:
            print('Sending getrtc command to parsivel ', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to getrtc from parsivel was ', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def setstationname(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        # max of 10 letter allowed
        sname = self.stationname[:10]
        cmd = b'CS/K/'+bytes(sname.encode(self.codec))
        if not self.quiet:
            print('Sending setstationname command to parsivel', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to setstationname ({sname}) from parsivel was', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def getstationname(self):

        if not self.isOpen():
            self.open()
            time.sleep(1)

        time.sleep(0.2)
        # max of 10 letter allowed
        sname = self.stationname[:10]
        cmd = b'CS/K\r'
        if not self.quiet:
            print('Sending getstationname command to parsivel', cmd)
        update = self.write(cmd)
        self.flush()
        time.sleep(2)
        answer = b''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            print('Answer to getstationname from parsivel was ', answer)
        self.flush()
        return answer.strip(b'\r\nOK\r\n\n').decode(self.codec).strip()

    def setdatetime(self):
        if not self.isOpen():
            self.open()
            time.sleep(1)

        self.setrtc()
        self.setdate()
        self.settime()

    def setup(self):
        if not self.isOpen():
            self.open()
            time.sleep(1)

        #sname = self.getstationname()
        self.setstationname()
        self.setdatetime()
        self.flush()

    def pollcode(self, code):
        if not self.isOpen():
            self.open()
            time.sleep(1)

        self.flush()
        self.clearbuffer()

        thispollcmd = str(code)

        if int(thispollcmd) >= 90:
            delim = ';'
            sleeptime = 1
        else:
            delim = ''
            sleeptime = 1

        thispollcmd = (thispollcmd+delim).encode(self.codec)
        pollcmd = b'CS/R/' + bytes(thispollcmd)+b'\r\n'
        written = self.write(pollcmd)
        #self.flush()
        # according to manual there is a guarantee that the parsivel answers within 500 ms
        # so we wait here to ensure the buffer is full
        time.sleep(sleeptime)
        self.polled = True
        while self.in_waiting == 0 or self.waittime <= sleeptime:

            time.sleep(self.waitdt)
            self.waittime += self.waitdt

            if self.waittime > self.maxwait:
                if not self.quiet:
                    print(f'Breaking out of waiting for answer on serial as no data arrived after {self.maxwait} seconts!!!')
                break

        answer = ''
        if self.in_waiting > 0:
            answer = self.read_until() #size=self.in_waiting)
            answer = answer.decode(self.codec)
        self.flush()
        self.polled = False

        return answer

    def help(self):
        if not self.isOpen():
            self.open()
            time.sleep(1)

        self.flush()
        self.clearbuffer()
        pollcmd = b'CS/?\r\n'
        written = self.write(pollcmd)
        self.flush()
        # according to manual there is a guarantee that the parsivel answers within 500 ms
        # so we wait here to ensure the buffer is full
        time.sleep(1)
        while self.in_waiting == 0 or self.waittime <= 0.5:

            time.sleep(self.waitdt)
            self.waittime += self.waitdt

            if self.waittime > self.maxwait:
                if not self.quiet:
                    print(f'Breaking out of waiting for answer on serial as no data arrived after {self.maxwait} seconts!!!')
                break

        answer = ''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            answer = answer.decode(self.codec)

        print(answer)


    def getconfig(self):
        if not self.isOpen():
            self.open()
            time.sleep(1)

        self.clearbuffer()
        pollcmd = b'CS/L\r' 
        written = self.write(pollcmd)
        # according to manual there is a guarantee that the parsivel answers within 500 ms
        # so we wait here to ensure the buffer is full
        time.sleep(0.5)
        while self.in_waiting == 0 or self.waittime <= 0.5:

            time.sleep(self.waitdt)
            self.waittime += self.waitdt

            if self.waittime > self.maxwait:
                if not self.quiet:
                    print(f'Breaking out of waiting for answer on serial as no data arrived after {self.maxwait} seconts!!!')
                break

        answer = ''
        if self.in_waiting > 0:
            answer = self.read(size=self.in_waiting)
            answer = answer.decode(self.codec)
            #answer = answer.split('\r\n')

        print(answer)
        return answer

    def poll(self):
        if not self.isOpen():
            self.open()
            time.sleep(1)

        self.flush()
        self.clearbuffer()

        written = self.write(self.pollcmd)
        # according to manual there is a guarantee that the parsivel answers within 500 ms
        # so we wait here to ensure the buffer is full
        time.sleep(0.5)
        self.polled = True

    def clearbuffer(self):
        # reset buffer in any case
        self.buffer = b''

    def cleardata(self):
        # cleanup data dict after we've written out everything usually
        self.data = {'-1': []}

    def clear(self):
        self.clearbuffer()
        self.cleardata()

    def velocity_classes(self):
        """
        Return arrays of relevant velocity classes for use with TROPOS nc.

        Hardcoded velocity bins of parsivel are used to construct:
            1. velocitybin The sizes as lower -> upper edge
            2. velocities as the the average velocity of a bin
            3. the raw_velocities that have been used to construct the above 2

        Returns
        -------
        velocitybin : array of float
            The droplet sizes .
        velocities : array of float
            The bin widths as difference to lower and upper edge for each bin.
        raw_velocities : array of float
            The bin widths (raw as per manual).

        """

        raw_velocities = [0.0] + \
            [0.1] * 10 + \
            [0.2] * 5 + \
            [0.4] * 5 + \
            [0.8] * 5 + \
            [1.6] * 5 + \
            [3.2] * 2

        velocities = np.asarray([(raw_velocities[i] + raw_velocities[i + 1])/2
                      for i in range(len(raw_velocities[:-1]))])

        velocitybin = np.cumsum(velocities)

        return velocitybin, velocities, np.asarray(raw_velocities)

    def diameter_classes(self, asmeters=True):
        """
        Return arrays of relevant diameter classes for use with TROPOS nc.

        Hardcoded bin widths of parsivel are used to construct:
            1. dropletsizes The sizes as lower -> upper edge
            2. dropletwidth as the the average of upper/lower edge
            3. the sizes that have been used to construct the above 2

        Returns
        -------
        dropletsizes : array of float
            The droplet sizes .
        dropletwidths : array of float
            The bin widths as difference to lower and upper edge for each bin.
        raw_dropletwidths : array of float
            The bin widths (raw as per manual).

        """

        raw_dropletwidths = [0.0] + \
            [0.125] * 10 + \
            [0.250] * 5 + \
            [0.500] * 5 + \
            [1] * 5 + \
            [2] * 5 + \
            [3] * 2

        dropletwidths = [(raw_dropletwidths[i]+raw_dropletwidths[i+1])/2
                         for i in range(len(raw_dropletwidths[:-1]))]

        dropletsizes = np.cumsum(dropletwidths)
        if asmeters:
            scaling = 1000
        else:
            scaling = 1
        return dropletsizes / scaling, np.asarray(dropletwidths) / scaling, np.asarray(raw_dropletwidths) / scaling

    # max sampling time in seconds (to be restarted by cronjob
    def sample(self, writeoutfreq=None):
        self.setup()

        if writeoutfreq is None:
            writeoutfreq = self.samplinginterval

        if writeoutfreq % self.samplinginterval != 0:
            print(f'Writoutfreq has been adjusted to be the lower multiple of the samplinginterval {self.samplinginterval}')
            writeoutfreq = (writeoutfreq // self.samplinginterval) * self.samplinginterval

        parsivel.reset_input_buffer()
        time.sleep(1)

        curdt = 0
        try:
            while curdt <= self.maxsampling or self.maxsampling <0:
                parsivel.getparsiveldata()
                #if curdt % 60 == 0:
                if curdt % writeoutfreq == 0:
                    parsivel.write2file()
                time.sleep(self.samplinginterval)
                curdt += self.samplinginterval
        except serial.SerialException:
            print('Issue with serial connection encounted, rerun...')
        except KeyboardInterrupt:
            print('Sampling interrupted.')

    def getparsiveldata(self):
        if not self.isOpen():
            self.open()

        now = datetime.datetime.utcnow()

        self.flush()
        time.sleep(0.1)

        if not self.polled:
            self.poll()

        while self.in_waiting > 0 or self.waittime <= 0.5:
            self.buffer += self.read(size=self.in_waiting)

            curbytes = self.in_waiting

            time.sleep(self.waitdt)
            self.waittime += self.waitdt

            if self.waittime > self.maxwait:
                if not self.quiet:
                    print(f'Breaking out of waiting for answer on serial as no data arrived after {self.maxwait} seconts!!!')
                break

            # since we check after the time.sleep we can assume if there is nothing new that we are done
            if curbytes == self.in_waiting and curbytes > 1:
                if not self.quiet:
                    print(f'Breaking out of waiting for answer on serial as no new data has arrived after one more time step of {self.waitdr} after {self.waittime}!')
                break
        else:
            if len(self.buffer) == 0:
                if not self.quiet:
                    print(f'No bytes were available to read after {self.maxwait} seconds and we waited {self.waittime} seconds for an answer. ')
            elif len(self.buffer) > 1:
                if not self.quiet:
                    print(f'{len(self.buffer)} bytes have been read in {self.waittime} seconds. ')
            else:
                pass

        if not self.quiet:
            print('Received the following answer to poll:\n', self.buffer)

        self.waittime = 0

        self.polled = False

        # convert to sensible string
        record = self.buffer.strip(b'\x03').decode(self.codec).strip()
        # get different fields into list
        record = record.split('\r\n')
        # split into measurement value key and measurement value
        # the default return is CODE (2 Letters): data (until prev. removed \r\n
        record = {i[:2]: i[3:].rstrip(';').strip() for i in record[1:]}

        for key, value in sorted(record.items()):
            # maintenance codes
            if key in ['94', '95', '96', '97', '98', '99']:
                continue

            # build up the dict to hold the available data
            if key not in self.data:
                self.data[key] = []

            # date, time, software versions that should not be converted
            # as well as synop codes, sensor date/time and measuring start 
            # as we handle these ourselves
            if key in ['20', '21', '14', '15', '05', '06', '19', '21', '22']:
                pass

            # spectra data
            elif key in ['90', '91',  '93']:
                value = value.replace('000','')

                if value.count(';') == len(value):
                    value = np.zeros((32, 32))
                else:
                    # spectra numbers are int
                    if key in ['93']:
                        value = [int(i) if i else 0 for i in value.split(';')]
                    # others are float
                    elif key in ['90', '91']:
                        value = [float(i) if i else 0 for i in value.split(';')]

                    value = np.asarray(value)
                    if key in ['93']:
                        value = value.reshape(32, 32)
            else:
                # float
                if '.' in value and value.count('.') == 1:
                    value = float(value)
                else:
                    # maybe integer?
                    try:
                        value = int(value)
                    # neither float nor integer, maybe a weather code, like wawa
                    except ValueError:
                        print(f'Conversion to int failed for {value}, based on {key}')
                        pass

            self.data[key] += [value]

        # replace sensor time with system time
        # 21 = date, 20 = time
        self.data['21'][-1] = now.strftime('%d.%m.%Y')
        self.data['20'][-1] = now.strftime('%H:%M:%S')

        # keep unix time seperate
        self.data['-1'] += [datetime.datetime.timestamp(now)]

        # cleanup buffer
        self.clearbuffer()

    def write2file(self, *args, **kwargs):
        self.write2asdofile(*args, **kwargs)
        self.write2ncfile(*args, **kwargs)
        self.clear()

    def _setupncfile(self):
        if os.path.exists(self.ncfile):
            nchandle = nc.Dataset(self.ncfile, 'a', format='NETCDF3_CLASSIC')
            return nchandle

        if not self.quiet:
            print(f'Setting up {outfile}')

        nchandle = nc.Dataset(self.ncfile, 'w', format='NETCDF3_CLASSIC')

        nchandle.createDimension('time', None)
        nchandle.createDimension('diameter', 32)
        nchandle.createDimension('velocity', 32)
        nchandle.createDimension('nv', 2)

        for key, value in self.ncmeta.items():
            setattr(nchandle, key, value)

        now = datetime.datetime.utcnow()
        setattr(nchandle, "Processing_date", str(datetime.datetime.utcnow()) + ' (UTC)')

        datavar = nchandle.createVariable('lat', 'd', ())
        setattr(datavar, 'standard_name', 'latitude')
        setattr(datavar, 'long_name', 'Latitude of instrument location')
        setattr(datavar, 'units', 'degrees_north')
        datavar.assignValue(self.ncmeta['latitude'])

        datavar = nchandle.createVariable('lon', 'd', ())
        setattr(datavar, 'standard_name', 'longitude')
        setattr(datavar, 'long_name', 'Longitude of instrument location')
        setattr(datavar, 'units', 'degrees_east')
        datavar.assignValue(self.ncmeta['longitude'])

        datavar = nchandle.createVariable('zsl', 'd', ())
        setattr(datavar, 'standard_name', 'altitude')
        setattr(datavar, 'long_name',
                'Altitude of instrument sensor above mean sea level')
        setattr(datavar, 'units', 'm')
        datavar.assignValue(self.ncmeta['altitude'])

        datavar = nchandle.createVariable('time', 'i', ('time',))
        setattr(datavar, 'standard_name', 'time')
        setattr(datavar, 'long_name',
                'Unix time at start of data transfer in seconds after 00:00 UTC on 1/1/1970')
        setattr(datavar, 'units', 'seconds since 1970-01-01 00:00:00')
        setattr(datavar, 'bounds', 'time_bnds')
        setattr(datavar, 'comment',
                'Time on data acquisition pc at initialization of serial connection to Parsivel.')

        datavar = nchandle.createVariable('time_bnds', 'i', ('time', 'nv'))
        setattr(datavar, 'units', 's')
        setattr(datavar, 'comment', 'Upper and lower bounds of measurement interval.')

        datavar = nchandle.createVariable('interval', 'i', ('time',))
        setattr(datavar, 'long_name', 'Length of measurement interval')
        setattr(datavar, 'units', 's')
        setattr(datavar, 'comment',
                'Variable 09 - Sample interval between two data retrieval requests.')


        diameters  = self.diameter_classes()
        datavar = nchandle.createVariable('diameter', 'd', ('diameter',))
        setattr(datavar, 'long_name', 'Center diameter of precipitation particles')
        setattr(datavar, 'units', 'm')
        setattr(datavar, 'comment',
                'Predefined diameter classes. Note the variable bin size.')
        datavar[:] = diameters[0]

        datavar = nchandle.createVariable('diameter_spread', 'd', ('diameter',))
        setattr(datavar, 'long_name', 'Width of diameter interval')
        setattr(datavar, 'units', 'm')
        setattr(datavar, 'comment', 'Bin size of each diameter class.')
        datavar[:] = (diameters[1])

        datavar = nchandle.createVariable('diameter_bnds', 'i', ('diameter', 'nv'))
        setattr(datavar, 'units', 'm')
        setattr(datavar, 'comment', 'Upper and lower bounds of diameter interval.')
        datavar[:, :] = np.stack([np.cumsum(diameters[2][:-1]), np.cumsum(diameters[2][1:])]).T

        velocities = self.velocity_classes()

        datavar = nchandle.createVariable('velocity', 'd', ('velocity',))
        setattr(datavar, 'long_name',
                'Center fall velocity of precipitation particles')
        setattr(datavar, 'units', 'm s-1')
        setattr(datavar, 'comment',
                'Predefined velocity classes. Note the variable bin size.')
        datavar[:] = (velocities[0])

        datavar = nchandle.createVariable('velocity_spread', 'd', ('velocity',))
        setattr(datavar, 'long_name', 'Width of velocity interval')
        setattr(datavar, 'units', 'm')
        setattr(datavar, 'comment', 'Bin size of each velocity interval.')
        datavar[:] = (velocities[1])

        datavar = nchandle.createVariable('velocity_bnds', 'd', ('velocity', 'nv'))
        setattr(datavar, 'comment', 'Upper and lower bounds of velocity interval.')
        datavar[:, :] = np.stack([np.cumsum(velocities[2][:-1]), np.cumsum(velocities[2][1:])]).T


        datavar = nchandle.createVariable(
            'data_raw', 'd', ('time', 'diameter', 'velocity',), fill_value=-999.)
        setattr(datavar, 'long_name',
                'Raw Data as a function of particle diameter and velocity')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment', 'Variable 93 - Raw data.')

        datavar = nchandle.createVariable(
            'number_concentration', 'd', ('time', 'diameter',), fill_value=-999.)
        setattr(datavar, 'long_name', 'Number of particles per diameter class')
        setattr(datavar, 'units', 'log10(m-3 mm-1)')
        setattr(datavar, 'comment', 'Variable 90 - Field N (d)')

        datavar = nchandle.createVariable(
            'fall_velocity', 'd', ('time', 'diameter',), fill_value=-999.)
        setattr(datavar, 'long_name', 'Average velocity of each diameter class')
        setattr(datavar, 'units', 'm s-1')
        setattr(datavar, 'comment', 'Variable 91 - Field v (d)')

        datavar = nchandle.createVariable('n_particles', 'i', ('time',))
        setattr(datavar, 'long_name', 'Number of particles in time interval')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment', 'Variable 11 - Number of detected particles')

        datavar = nchandle.createVariable(
            'rainfall_rate', 'd', ('time',), fill_value=-999.)
        setattr(datavar, 'standard_name', 'rainfall_rate')
        setattr(datavar, 'long_name', 'Precipitation rate')
        setattr(datavar, 'units', 'm s-1')
        setattr(datavar, 'comment', 'Variable 01 - Rain intensity (32 bit) 0000.000')

        datavar = nchandle.createVariable(
            'radar_reflectivity', 'd', ('time',), fill_value=-999)
        setattr(datavar, 'standard_name', 'equivalent_reflectivity_factor')
        setattr(datavar, 'long_name', 'equivalent radar reflectivity factor')
        setattr(datavar, 'units', 'dBZ')
        setattr(datavar, 'comment', 'Variable 07 - Radar reflectivity (32 bit).')

        datavar = nchandle.createVariable('E_kin', 'd', ('time',), fill_value=-999.)
        setattr(datavar, 'long_name', 'Kinetic energy of the hydrometeors')
        setattr(datavar, 'units', 'kJ')
        setattr(datavar, 'comment', 'Variable 24 - kinetic Energy of hydrometeors.')

        datavar = nchandle.createVariable(
            'visibility', 'i', ('time',), fill_value=-999)
        setattr(datavar, 'long_name', 'Visibility range in precipitation after MOR')
        setattr(datavar, 'units', 'm')
        setattr(datavar, 'comment',
                'Variable 08 - MOR visibility in the precipitation.')

        datavar = nchandle.createVariable(
            'synop_WaWa', 'i', ('time',), fill_value=-999)
        setattr(datavar, 'long_name', 'Synop Code WaWa')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment',
                'Variable 03 - Weather code according to SYNOP wawa Table 4680.')

        datavar = nchandle.createVariable(
            'synop_WW', 'i', ('time',), fill_value=-999)
        setattr(datavar, 'long_name', 'Synop Code WW')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment',
                'Variable 04 - Weather code according to SYNOP ww Table 4677.')

        datavar = nchandle.createVariable(
            'T_sensor', 'i', ('time',), fill_value=-999)
        setattr(datavar, 'long_name', 'Temperature in the sensor')
        setattr(datavar, 'units', 'K')
        setattr(datavar, 'comment', 'Variable 12 - Temperature in the Sensor')

        datavar = nchandle.createVariable('sig_laser', 'i', ('time',))
        setattr(datavar, 'long_name', 'Signal amplitude of the laser')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment',
                'Variable 10 - Signal ambplitude of the laser strip')

        datavar = nchandle.createVariable('state_sensor', 'i', ('time',))
        setattr(datavar, 'long_name', 'State of the Sensor')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment', 'Variable 18 - Sensor status:\n'\
                                    '0: Everything is okay.\n' \
                                    '1: Dirty but measurement possible.\n'\
                                    '2: No measurement possile')

        datavar = nchandle.createVariable('V_sensor', 'd', ('time',))
        setattr(datavar, 'long_name', 'Sensor Voltage')
        setattr(datavar, 'units', 'V')
        setattr(datavar, 'comment', 'Variable 17 - Power supply voltage in the sensor.')

        datavar = nchandle.createVariable('I_heating', 'd', ('time',))
        setattr(datavar, 'long_name', 'Heating Current')
        setattr(datavar, 'units', 'A')
        setattr(datavar, 'comment', 'Variable 16 - Current through the heating system.')

        datavar = nchandle.createVariable('error_code', 'i', ('time',))
        setattr(datavar, 'long_name', 'Error Code')
        setattr(datavar, 'units', '1')
        setattr(datavar, 'comment', 'Variable 25 - Error code.')

        return nchandle


    def write2ncfile(self, intosubdirs=True, ):

        if self.data:
            pass
        else:
            if not self.quiet:
                print('No data have been read yet. Call getparsiveldata() first.')
            return

        if not self.outpath.endswith(os.sep):
            self.outpath += os.sep

        os.makedirs(self.outpath, exist_ok=True)

        udays = sorted(list(set(self.data['21'])))
        for day in udays:
            if intosubdirs:
               ymd = day.split('.')[::-1]
               ymd = [i + j for i, j in zip(['Y', 'M', 'D'], ymd)]
               _outpath = self.outpath+os.sep.join(ymd)+os.sep
               os.makedirs(_outpath, exist_ok=True)
            else:
                _outpath = self.outpath

            # day has the format d.m.Y but we want the filename to be Ymd
            outfile = self.fileprefix +''.join(day.split('.')[::-1]) + '.nc'
            self.ncfile = _outpath + outfile
            nchandle = self._setupncfile()
            setattr(nchandle, 'Date', day)

            index_of_day = [i[0] for i in enumerate(self.data['21']) if i[1] == day]

            curtimestep = nchandle.dimensions['time'].size

            unixtime = ([self.data['-1'][i] for i in index_of_day])
            nchandle.variables["time"][curtimestep] = (unixtime)
            bnds = [[self.data['-1'][i] - int(self.data['09'][i]), self.data['-1'][i]] for i in index_of_day]
            nchandle.variables['time_bnds'][curtimestep, :] = (bnds)

            varNames = nchandle.variables.keys()

            for ncvar in self.ncmapping:
                thisvar = nchandle.variables[self.ncmapping[ncvar]]
                thisdata = [self.data[ncvar][i] for i in index_of_day]
                if ncvar in self.ncscaling:
                     thisdata = [i * self.ncscaling[ncvar] for i in thisdata]

                if len(thisvar.shape) == 1:
                    thisvar[curtimestep] = (thisdata)
                elif len(thisvar.shape) == 2:
                    thisvar[curtimestep, :] = (thisdata)
                elif len(thisvar.shape) == 3:
                    thisdata = np.asarray(thisdata).reshape(thisvar.shape[1:])
                    thisvar[curtimestep, :, :] = (thisdata)

            nchandle.close()
            now = datetime.datetime.utcnow()

            print(f'Written {len(index_of_day)} records of data to {self.ncfile} at {now}')
            self.ncfiles = list(set(self.ncfiles+[self.ncfile]))
        pass

    # order can be anything, but defaults to ASDO format, see header in below function
    def write2asdofile(self, intosubdirs=True, varorder=[], header=[]):
        assert len(varorder) == len(header), 'Order of variables and header have to match'

        if self.data:
            pass
        else:
            print('No data have been read yet. Call getparsiveldata() first.')
            return

        if varorder:
            self.csvoutputorder = varorder

        if header:
            self.csvheader = header

        if not self.outpath.endswith(os.sep):
            self.outpath += os.sep

        os.makedirs(self.outpath, exist_ok=True)
        filemode = 'a'

        # examples ASDO file
        #04.03.2023,00:00:00,0.000,216.57,0,NP,C,-9.999,20000,19866,0,-1,0.64,23.8,0,0.000,0,<SPECTRUM>ZERO</SPECTRUM>
        #2023.03.28,14:30:58,0.0,33.06,0,NP,C,-9.999,20000,21649,0,16,0.0,23.8,0,0.0,0.0,<SPECTRUM></SPECTRUM>
        # 25.02.2023,00:05:30,3.100,210.07,62,RA,R,32.661,8290,16569,90,1,0.00,23.8,0,53.310,0,
        # <SPECTRUM>,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        # ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        # ,,,,,,,,,,,,,,,,,,,,2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        #,,,,,,,,,2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        #,1,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,5,2,2,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,
        #4,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,1,5,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2,1,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,1,,3,
        #1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2,,4,10,7,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,3,8,3,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,4,
        #2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,1,,,,,,,,,,,,
        #,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        #,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,</SPECTRUM>
        udays = sorted(list(set(self.data['21'])))

        for day in udays:
            if intosubdirs:
               ymd = day.split('.')[::-1]
               ymd = [i + j for i, j in zip(['Y', 'M', 'D'], ymd)]
               _outpath = self.outpath+os.sep.join(ymd)+os.sep
               os.makedirs(_outpath, exist_ok=True)
            else:
                _outpath = self.outpath

            # day has the format d.m.Y but we want the filename to be Ymd
            self.csvfile = self.fileprefix +''.join(day.split('.')[::-1]) + '.csv'
            writeheader = True

            # write out the buffer to file
            if os.path.exists(_outpath + self.csvfile):
                writeheader = False

            # maxtimesteps because self.data holds everything
            ntimesteps = len(self.data['20'])

            with open(_outpath+self.csvfile, filemode) as fo:

                if writeheader:
                     fo.write(','.join(self.csvheader))
                     fo.write('\n')

                for timestep in range(ntimesteps):
                    # skip if not the same day
                    if self.data['21'][timestep] != day:
                        continue
                    for key in self.csvoutputorder:
                        varrec = self.data[key][timestep]
                        if key in '93':
                            fo.write('<SPECTRUM>')

                        if key in ['90', '91', '93']:
                            if not isinstance(varrec, str):
                                varrec = ','.join([str(i) if i > 0 else '' for i in varrec.flatten()])

                            if len(varrec) == varrec.count(','):
                                varrec = 'ZERO'

                        fo.write(str(varrec))

                        if key in '93':
                            fo.write('</SPECTRUM>')
                        else:
                            fo.write( ',')

                    fo.write('\n')
            self.csvfiles = list(set(self.csvfiles+[self.csvfile]))
            if not self.quiet:
                print(f'Written {ntimesteps} records to {_outpath+self.outfile} for {day}')

if __name__ == '__main__':
    parsivel = parsivel_moxa(outpath='/media/data/parsivel/',)
    #parsivel.help()
    #parsivel.pollcode(33)
    #parsivel.pollcode(93)
    try:
        parsivel.sample()
    except KeyboardInterrupt:
        print('Sampling interrupted.')

    #time.sleep(1)
    #cfg = parsivel.getconfig()
    #print(cfg)
    #time.sleep(1)
    #curdt = 0
    #try:
    #    while curdt <= maxdt :
    #        parsivel.getparsiveldata()
    #        #if curdt % 60 == 0:
    #        parsivel.write2file()
    #        time.sleep(dt)
    #        curdt += dt
    #except serial.SerialException:
    #    print('Issue with serial connection encounted, rerun...')
    #    del parsivel
    #finally:
    #    del parsivel
