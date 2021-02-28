from pymsfilereader import MSFileReader
from MPMF_Database_SetUp import MPMFDBSetUp
from MPMF_File_System import FileSystem
import numpy as np
import json
import os
import matplotlib.pyplot as plt
import math

# NOTE: pymsfilereader not on conda
# take out of yaml file (config via pip!)
# PLUS: add bs4 to yaml

class ThermoMetrics:

    def __init__(self, filepath, filename, exp, db):

        self.exp = exp
        self.filepath = filepath
        self.filename = filename
        self.rawfile = MSFileReader(self.filepath)
        self.db = db
        self.db.db.commit() # commit for any other instances
        self.run_id = self.db.get_run_id(self.filename)
        self.comp_id = self.set_component_id()

        # change path to store plots (remove before use)
        #self.plt_path = "C:\\Users\\sliggady\\Desktop\\OutfilesTest\\" + exp + "\\Plots\\" + self.filename
        #if not os.path.isdir(self.plt_path):
            #os.makedirs(self.plt_path)
        #os.chdir(self.plt_path)

        if self.run_id:
            if not self.rawfile.IsError():
                # set controller type 4 for UV, 2 for A/D
                if self.rawfile.GetNumberOfControllersOfType('UV') > 0:
                    self.controller_type = 4
                elif self.rawfile.GetNumberOfControllersOfType('A/D card') > 0:
                    self.controller_type = 2
                else:
                    print("No Profile Data")

                if self.exp.upper() == "PROTEOMICS":
                    # set controllers
                    self.lp_cont = 1
                    self.np_cont = 2
                    self.co_cont = 3

                    # valve limits
                    self.first_valve = 2 # get from config file
                    self.last_valve = 3 # get from config file

                    # get data from raw
                    self.lp_data = self.set_lp_data()
                    self.np_data = self.set_np_data()
                    self.co_data = self.set_co_data()
                elif self.exp == "METABOLOMICS":
                    # controller and data
                    self.mp_cont = 1
                    self.mp_data = self.set_mp_data()
                else:
                    print("Wrong experiment type " + self.exp)
            else:
                print('MSFileReader Error', self.rawfile.GetErrorMessage())
        else:
            print("Run not entered")

        self.run()
        self.rawfile.Close()

    def run(self):
        if self.exp.upper() == "PROTEOMICS":
            # get and insert metrics
            self.all_loading_pump()
            self.all_nano_pump()
            self.insert_co_temp()
            self.insert_pressure_profile("np")
            self.insert_pressure_profile("lp")
        else:
            self.all_main_pump()
            self.insert_pressure_profile("mp")

        print("INSTRUMENT METRICS INSERTED FOR " + self.filename)
        print("PROFILE DATA INSERTED FOR " + self.filename)

    def set_component_id(self):
        # set the id needed for inserts based on experiment
        # may need to get these comp names from file for config purposes

        if self.exp == "METABOLOMICS":
            sql = "SELECT component_id FROM sample_component WHERE component_name = 'Metab Digest'"
        elif self.exp == "PROTEOMICS":
            sql = "SELECT component_id FROM sample_component WHERE component_name = 'Hela Digest'"

        try:
            self.db.cursor.execute(sql)
            return self.db.cursor.fetchone()[0]
        except Exception as e:
            print(e)
            return False

    def set_lp_data(self):
        self.rawfile.SetCurrentController(self.controller_type, self.lp_cont)
        return self.rawfile.GetChroData(startTime=self.rawfile.StartTime, endTime=self.rawfile.EndTime)

    def set_np_data(self):
        self.rawfile.SetCurrentController(self.controller_type, self.np_cont)
        return self.rawfile.GetChroData(startTime=self.rawfile.StartTime, endTime=self.rawfile.EndTime)

    def set_co_data(self):
        # check if column oven data first
        if self.rawfile.GetNumberOfControllersOfType('UV') > 2:
            self.rawfile.SetCurrentController(self.controller_type, self.co_cont)
            return self.rawfile.GetChroData(startTime=self.rawfile.StartTime, endTime=self.rawfile.EndTime)
        return []

    def set_mp_data(self):
        self.rawfile.SetCurrentController(self.controller_type, self.mp_cont)
        return self.rawfile.GetChroData(startTime=self.rawfile.StartTime, endTime=self.rawfile.EndTime)

    # LOADING PUMP - PROTEOMICS
    def create_lp_starting_bp(self):
        """Ave Pressure in first 2min (config first valve)"""
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # find 2-min index (first_valve)
        cut = 0
        for i in range(len(x)):
            if x[i] >= self.first_valve:
                cut = i
                break
        #self.plot_loading_pump(0, cut, "S")
        return np.mean(y[:cut])

    def create_lp_end_pressure(self):
        """Ave Pressure in last 3min (config last valve)"""
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # find last 3 min
        cut = 0
        ep_limit = x[len(x)-1] - self.last_valve
        for i in range(len(x)-1, -1, -1):
            if x[i] <= ep_limit:
                cut = i
                break
        #self.plot_loading_pump(cut, len(x), "E")
        return np.mean(y[cut:len(y)])

    def create_lp_air_injection(self):
        """Diff. b/w first reading and Max reading in first 60 sec"""
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # find 60sec index
        cut = 0
        air_limit = 1
        for i in range(len(x)):
            if x[i] >= air_limit:
                cut = i
                break

        first = y[0]
        max_60 = np.max(y[:cut])

        return max_60 - first

    def create_lp_valve_spike(self):
        # NOT USED
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # find 2-min index (starting pressure)
        cut = 0
        for i in range(len(x)):
            if x[i] >= self.first_valve:
                cut = i
                break

        # find last 3 min (end pressure)
        cut2 = 0
        ep_limit = x[len(x) - 1] - self.last_valve
        for i in range(len(x) - 1, -1, -1):
            if x[i] <= ep_limit:
                cut2 = i
                break

        x_start = x[:cut]
        y_start = y[:cut]
        x_end = x[cut2:len(x)]
        y_end = y[cut2:len(y)]

        # loop every data point (1st 2min) and get max in 10 sec windows
        valve_spike = 0
        for i in range(len(x_start)):
            if x_start[len(x_start)-1] - x_start[i] > 1/6:
                j = i + 1
                # find 10 sec window index
                while x_start[j] < (x_start[i] + 1/6):
                    j += 1

                y_max = np.max(y_start[i:j+1])
                temp = y_max - y_start[i]
            else:
                y_max = np.max(y_start[i:len(y_start)])
                temp = y_max - y_start[i]

            if temp > valve_spike:
                valve_spike = temp

        # loop every data point (last 3min) and get max in 10 sec windows
        for i in range(len(x_end)):
            if x_end[len(x_end) - 1] - x_end[i] > 1 / 6:
                j = i + 1
                # find 10 sec window index
                while x_end[j] < (x_end[i] + 1 / 6):
                    j += 1

                y_max = np.max(y_end[i:j + 1])
                temp = y_max - y_end[i]
            else:
                y_max = np.max(y_end[i:len(y)])
                temp = y_max - y_end[i]

            if temp > valve_spike:
                valve_spike = temp

        return valve_spike

    def create_lp_valve_spike_start(self):
        """Creates a 10 sec window around the max starting pressure
            and returns abs value of this range
        """
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # find 2-min index (starting pressure)
        cut = 0
        for i in range(len(x)):
            if x[i] >= self.first_valve:
                cut = i
                break

        x_start = x[:cut]
        y_start = y[:cut]

        # find max and index of max
        max_start = np.max(y_start)
        max_index = np.where(y_start == max_start)[0][0]

        # find index 5 sec before
        start_index = max_index
        while start_index > 0 and x_start[max_index] - x_start[start_index] < 1/12:
            start_index -= 1

        # find index 5 sec after
        end_index = max_index
        while end_index < len(x_start)-1 and x_start[end_index] - x_start[max_index] < 1/12:
            end_index += 1

        #self.plot_loading_pump(start_index, end_index, "VS")
        return abs(y_start[start_index] - y_start[end_index])

    def create_lp_valve_spike_end(self):
        """Creates a 10 sec window around the max end pressure
            and returns abs value of this range
        """
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # find last 3 min
        cut = 0
        ep_limit = x[len(x) - 1] - self.last_valve
        for i in range(len(x) - 1, -1, -1):
            if x[i] <= ep_limit:
                cut = i
                break

        x_end = x[cut:len(x)]
        y_end = y[cut:len(y)]

        # find max and index of max
        max_start = np.max(y_end)
        max_index = np.where(y_end == max_start)[0][0]

        # find index 5 sec before
        start_index = max_index
        while start_index > 0 and x_end[max_index] - x_end[start_index] < 1/12:
            start_index -= 1

        # find index 5 sec after
        end_index = max_index
        while end_index < len(x_end)-1 and x_end[end_index] - x_end[max_index] < 1/12:
            end_index += 1

        #self.plot_loading_pump(start_index+cut, end_index+cut, "VE")
        return abs(y_end[start_index] - y_end[end_index])

    # NANO PUMP - PROTEOMICS
    def create_np_starting_pressure(self):
        """Median Pressure in first 2min"""
        x = np.array(self.np_data[0][0])
        y = np.array(self.np_data[0][1])

        # find 2-min index
        cut = 0
        sp_limit = 2
        for i in range(len(x)):
            if x[i] >= sp_limit:
                cut = i
                break

        #self.plot_nano_pump(0, cut)
        return np.median(y[:cut])

    def create_np_valve_drop(self):
        """Difference b/w starting pressure and first trough"""
        x = np.array(self.np_data[0][0])
        y = np.array(self.np_data[0][1])

        # find 3-min index
        cut = 0
        dp_limit = 3
        for i in range(len(x)):
            if x[i] >= dp_limit:
                cut = i
                break

        # find min of 1st 3 min (the trough)
        y_start = y[:cut]
        y_trough = np.min(y_start)

        # find index of the trough
        trough_index = np.where(y_start == y_trough)

        #self.plot_nano_pump(0, trough_index[0][0])
        return y_trough, trough_index[0][0]

    def create_np_max_pressure(self):
        """Max Pressure"""
        y = np.array(self.np_data[0][1])
        return np.max(y)

    def create_np_min_pressure(self):
        """Min Pressure"""
        y = np.array(self.np_data[0][1])
        return np.min(y)

    def create_np_inline_leak(self):
        """Difference b/w starting pressure and first peak after first trough"""
        x = np.array(self.np_data[0][0])
        y = np.array(self.np_data[0][1])

        trough = self.create_np_valve_drop()
        trough_index = trough[1]
        cut = x[trough_index] + 4 # 4 minute after trough

        cut_index = 0
        for i in range(trough_index, len(x) - 1):
            if x[i] > cut:
                cut_index = i
                break

        #self.plot_nano_pump(trough_index, cut_index)
        return np.max(y[trough_index:cut_index])

    def create_np_pressure_diff(self):
        """Difference b/w median of first minute and median of last minute"""
        x = np.array(self.np_data[0][0])
        y = np.array(self.np_data[0][1])

        # find 1-min index
        cut = 0
        fm_limit = 1
        for i in range(len(x)):
            if x[i] >= fm_limit:
                cut = i
                break

        # find last 1 min index
        cut_last = 0
        lm_limit = x[len(x) - 1] - 1
        for j in range(len(x) - 1, -1, -1):
            if x[j] <= lm_limit:
                cut_last = j
                break

        #self.plot_nano_pump()
        return np.median(y[:cut]) - np.median(y[cut_last:len(y)])

    # COLUMN OVEN - PROTEOMICS
    def create_column_range(self):
        """Temperature range (celcius) of column oven"""
        if self.co_data:
            y = np.array(self.co_data[0][1])
            #self.plot_column_oven()
            diff = np.max(y) - np.min(y)
            if diff < 2:
                return np.max(y) - np.min(y)
            else: # else likely error
                return -1
        return -1

    # MAIN PUMP - METABOLOMICS
    def create_mp_max_pressure(self):
        """Max Pressure, returns max pressure and RT of max pressure"""
        x = np.array(self.mp_data[0][0])
        y = np.array(self.mp_data[0][1])

        max_y = np.max(y)
        max_index = np.where(y == max_y)[0][0]
        #self.plot_main_pump()
        return max_y, x[max_index]

    def create_mp_starting_pressure(self):
        """Median Pressure in first 1min"""
        x = np.array(self.mp_data[0][0])
        y = np.array(self.mp_data[0][1])

        # find 2-min index
        cut = 0
        mp_limit = 1
        for i in range(len(x)):
            if x[i] >= mp_limit:
                cut = i
                break

        #self.plot_main_pump()
        return np.median(y[:cut])

    def create_mp_end_pressure(self):
        """Ave Pressure in last 1min"""
        x = np.array(self.mp_data[0][0])
        y = np.array(self.mp_data[0][1])

        # find last 1 min
        cut = 0
        ep_limit = x[len(x)-1] - 1
        for i in range(len(x)-1, -1, -1):
            if x[i] <= ep_limit:
                cut = i
                break
        #self.plot_main_pump()
        return np.median(y[cut:len(y)])

    # PLOTS
    def plot_loading_pump(self, x_min=0, x_max=0, plot="A"):
        os.chdir(self.plt_path)
        x = np.array(self.lp_data[0][0])
        y = np.array(self.lp_data[0][1])

        # set plot range
        if plot != "A":
            x = x[x_min:x_max]
            y = y[x_min:x_max]

        # set plot header
        if plot == "S":
            header = "Starting Backpressure"
        elif plot == "VS":
            header = "Valve Spike Start"
        elif plot == "VE":
            header = "Valve Spike End"
        elif plot == "E":
            header = "End Pressure"
        else:
            header = "Loading Pump"

        fig, ax = plt.subplots()
        ax.plot(x, y, linewidth=1)
        ax.set(xlabel='Time (minutes)', ylabel='Pressure', title=header)
        fig.savefig(header)
        plt.show()

    def plot_nano_pump(self, x_min=0, x_max=0):
        os.chdir(self.plt_path)
        x = np.array(self.np_data[0][0])
        y = np.array(self.np_data[0][1])

        if x_max != 0 or x_min != 0:
            x = x[x_min:x_max]
            y = y[x_min:x_max]

        header = "Nano Pump"
        if x_min == 0 and x_max != 0:
            header = "Valve Drop"

        fig, ax = plt.subplots()
        ax.plot(x, y, linewidth=1)
        ax.set(xlabel='Time (minutes)', ylabel='Pressure', title=header)
        fig.savefig(header)
        plt.show()

    def plot_column_oven(self):
        x = np.array(self.co_data[0][0])
        y = np.array(self.co_data[0][1])

        fig, ax = plt.subplots()
        ax.plot(x, y, linewidth=1)
        ax.set(xlabel='Time (minutes)', ylabel='Temperature (C)', title='Column Oven')
        #fig.savefig("Column Oven " + self.filename)
        plt.show()

    def plot_main_pump(self, x_min=0, x_max=0):
        os.chdir(self.plt_path)
        x = np.array(self.mp_data[0][0])
        y = np.array(self.mp_data[0][1])

        if x_max != 0 or x_min != 0:
            x = x[x_min:x_max]
            y = y[x_min:x_max]

        header = "Main Pump Metabolomics"

        fig, ax = plt.subplots()
        ax.plot(x, y, linewidth=1)
        ax.set(xlabel='Time (minutes)', ylabel='Pressure', title=header)
        fig.savefig(header)
        plt.show()

    def plot_data_bins(self, pump):
        os.chdir(self.plt_path)

        data = self.create_data_bins(pump)
        x = np.array(data["rts"])
        y = np.array(data["intensities"])

        # convert rts intervals for plotting(back to minutes)

        sum = 0
        rts = []
        for k in range(0, len(x)):
            sum += x[k]
            rts.append(sum / 60.0)


        # plot normal for comparison
        if pump == "mp":
            self.plot_main_pump()
        elif pump == "np":
            self.plot_nano_pump()
        else:
            self.plot_loading_pump()

        fig, ax = plt.subplots()
        ax.plot(rts, y, linewidth=1)
        ax.set(xlabel='Time (minutes)', ylabel='Pressure (Average per Second)', title=pump + " BINNED")
        fig.savefig(pump + "_BINNED")
        plt.show()

    # GET ALL METRICS and INSERT
    def all_loading_pump(self):

        # get the values
        sbp = self.create_lp_starting_bp()
        ep = self.create_lp_end_pressure()
        ai = self.create_lp_air_injection()
        vss = self.create_lp_valve_spike_start()
        vse = self.create_lp_valve_spike_start()
        pd_lp = sbp - ep

        # set-up dict for metrics and insert
        metrics = {"sbp": sbp, "ep": ep, "ai": ai, "vss": vss, "vse": vse, "pd-lp": pd_lp}
        self.insert_metrics(metrics)
        print("Inserted Loading Pump")

    def all_nano_pump(self):

        # get the values
        sp = self.create_np_starting_pressure()
        vd_end = self.create_np_valve_drop()[0]
        vd = sp - vd_end
        il_end = self.create_np_inline_leak()
        il = sp - il_end
        maxp = self.create_np_max_pressure()
        minp = self.create_np_min_pressure()
        pd = self.create_np_pressure_diff()

        # set up dict for metrics and insert
        metrics = {"sp": sp, "vd": vd, "maxp": maxp, "minp": minp, "il": il, "pd": pd}
        self.insert_metrics(metrics)
        print("Inserted Nano Pump")

    def all_main_pump(self):
        mp_names = ["maxp-metab", "rt-maxp", "sp-metab", "ep-metab"]

        # get the values
        mp_max = self.create_mp_max_pressure()
        maxp = mp_max[0]
        rt = mp_max[1]
        sp = self.create_mp_starting_pressure()
        ep = self.create_mp_end_pressure()

        # set up dict for metrics
        metrics = {"maxp-metab": maxp, "rt-maxp": rt, "sp-metab": sp, "ep-metab": ep}
        self.insert_metrics(metrics)
        print("Inserted Main Pump")

    def insert_co_temp(self):
        co_temp = self.create_column_range()
        metrics = {"co": co_temp}
        self.insert_metrics(metrics)
        print("Inserted Column Temp")

    # INSERT
    def insert_metrics(self, metrics):
        # INSERT (called by "all" functions)
        for metric in metrics:
            sql = "SELECT metric_id FROM metric WHERE metric_name = '" + metric + "'"
            try:
                self.db.cursor.execute(sql)
                metric_id = self.db.cursor.fetchone()[0]
            except Exception as e:
                print(e)

            insert_sql = "INSERT INTO measurement VALUES('" + str(metric_id) + "','" + \
                         str(self.comp_id) + "','" + str(self.run_id) + "','" + str(metrics[metric]) + "')"

            try:
                self.db.cursor.execute(insert_sql)
            except Exception as e:
                print(e)

        self.db.db.commit()

    # PRESSURE PROFILES
    def create_data_bins(self, pump):

        # bin the profile data for storing
        precision = 2

        if pump == "mp":
            x = np.array(self.mp_data[0][0])
            y = np.array(self.mp_data[0][1])
        elif pump == "np":
            x = np.array(self.np_data[0][0])
            y = np.array(self.np_data[0][1])
        else: # lp
            x = np.array(self.lp_data[0][0])
            y = np.array(self.lp_data[0][1])

        # bin in seconds (average)
        x_sec = []
        y_sec = []
        iave = 0
        count = 0
        second = math.ceil(x[0] * 60)
        for k in range(0, len(x)):
            if x[k] * 60 > second:
                x_sec.append(second)
                y_sec.append(round(iave/count, precision))
                iave = 0
                count = 0
                second = math.ceil(x[k] * 60)

            iave += y[k]
            count += 1

        # check whether the last values have been stored in x_sec, y_sec
        if x_sec[-1] < second:
            x_sec.append(second)
            y_sec.append(round(iave/count, precision))

        #print(pump)
        #print(str(len(x_sec)) + " Data Points")
        #print(x_sec[:5])
        #print(x_sec[-5:])
        #print(y_sec[:5])
        #print(y_sec[-5:])

        # transform retention times to vector containing the first retention time and only the retention time delta for all following values
        x_sec = [x_sec[k] - x_sec[k - 1] if k > 0 else x_sec[0] for k in range(0, len(x_sec))]

        return {"rts": x_sec, "intensities": y_sec}

    def insert_pressure_profile(self, pump):

        # get the profile data, convert to json and insert
        data = self.create_data_bins(pump)
        json_data = json.dumps(data)

        sql = "INSERT INTO pressure_profile VALUES(NULL,'" + json_data + "','" + str(pump) + "','" + str(self.run_id) + "')"

        try:
            self.db.cursor.execute(sql)
        except Exception as e:
            print(e)

        self.db.db.commit()

    # TEST
    def check_json(self, pump):

        sql = "SELECT pressure_data FROM pressure_profile WHERE pump_type = '" + pump + \
                "' AND run_id = '" + str(self.run_id) + "'"

        try:
            self.db.cursor.execute(sql)
            profile = self.db.cursor.fetchall()
        except Exception as e:
            print(e)

        #print(type(profile[0][0]))
        profile_dict = json.loads(profile[0][0])
        print(len(profile_dict["intensities"]))

if __name__ == "__main__":
    # TEST SCRIPT
    filename = "Z:\\qc_automation\\fusion\\instrument_data\\HelaiRT1uL_190722200502.raw"
    exp = "PROTEOMICS"
    db_info = {"user": "root", "password": "raja2417", "database": "mpmfdb"}
    fs = FileSystem("Z:\\qc_automation\\fusion\\instrument_data", "", "", "")
    db = MPMFDBSetUp(db_info["user"], db_info["password"], db_info["database"], fs)

    # one test file
    pump = "np"
    path_array = filename.split('\\')
    id = path_array[len(path_array) - 1][:-4]  # REMOVE .RAW
    thermo = ThermoMetrics(filename, id, exp, db)
    #thermo.insert_pressure_profile(pump)
    thermo.check_json(pump)

    # all proteomics raws for testing
    #raw_files = glob.glob("Z:\\qc_automation\\qeplus1\\instrument_data\\" + '\\' + 'HelaiRT1ul_*.raw')
    #raw_files.sort(key=lambda t: os.path.getmtime(t), reverse=True)  # sort
    #raw_files = raw_files[:2]

    # metabolomics clayton
    #raw_files = glob.glob("Z:\\Metabolomics\\QC_runs\\C1_Clayton\\QC_Metabolomics_*.raw")
    # raw_files.sort(key=lambda t: os.path.getmtime(t), reverse=True)  # sort
    #raw_files = raw_files[:2]

    # metabolomics parkville
    #raw_files = glob.glob("Z:\\Metabolomics\\QC_runs\\C2_Parkville\\M_QC_*.raw")
    #raw_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)  # sort
    #raw_files = raw_files[:10]

    #print("Running " + str(len(raw_files)) + " files.")

    raw_files = []
    for file in raw_files:
        # may need to test file split below next run
        path_array = file.split('\\')
        id = path_array[len(path_array) - 1][:-4]  # REMOVE .RAW
        print(id)
        thermo = ThermoMetrics(file, id, exp, db)
        if exp == "PROTEOMICS":
            thermo.plot_data_bins("lp")
            thermo.plot_data_bins("np")
            """
            lp_sbp = thermo.create_lp_starting_bp()
            lp_ai = thermo.create_lp_air_injection()
            lp_ep = thermo.create_lp_end_pressure()
            lp_vs = thermo.create_lp_valve_spike()
            lp_vs_s = thermo.create_lp_valve_spike_start()
            lp_vs_e = thermo.create_lp_valve_spike_end()
            np_sp = thermo.create_np_starting_pressure()
            np_vd = thermo.create_np_valve_drop()[0]
            np_max = thermo.create_np_max_pressure()
            np_min = thermo.create_np_min_pressure()
            np_il = thermo.create_np_inline_leak()
            np_pd = thermo.create_np_pressure_diff()
            co_range = thermo.create_column_range()
            """
            #thermo.plot_loading_pump()
            #thermo.plot_nano_pump()
            '''
            print("LOADING PUMP")
            print("Starting Backpressure", lp_sbp)
            print("Air Injection", lp_ai)
            print("End Pressure", lp_ep)
            print("Pressure Differential", lp_sbp - lp_ep)
            print("Valve Spike", lp_vs)
            print("Valve Spike Start", lp_vs_s)
            print("Valve Spike End", lp_vs_e)
            print("\n")
            print("NANO PUMP")
            print("Starting Pressure", np_sp)
            print("Valve Drop", np_sp - np_vd)
            print("Max Pressure", np_max)
            print("Min Pressure", np_min)
            print("Inline Leak", np_sp - np_il)
            print("Pressure Differential", np_pd)
            print("\n")
            print("COLUMN OVEN TEMP")
            print(str(co_range) + " C")
            print("\n")
            '''
        else:
            '''
            mp_max = thermo.create_mp_max_pressure()
            mp_sp = thermo.create_mp_starting_pressure()
            mp_ep = thermo.create_mp_end_pressure()
            
            print("METAB MAIN PUMP")
            print("Max Pressure", mp_max[0])
            print("RT of Max Pressure", mp_max[1])
            print("Starting Pressure", mp_sp)
            print("End Pressure", mp_ep)
            print("\n")
            '''
            thermo.plot_data_bins("mp")
            #thermo.plot_main_pump()

