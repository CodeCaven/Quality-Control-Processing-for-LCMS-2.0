from MPMF_File_System import FileSystem
from MPMF_Database_SetUp import MPMFDBSetUp
from MPMF_Stats import Stat
from MPMF_Chromatogram import Chromatogram
from MPMF_Email import SendEmail
from MPMF_Thermo_Metrics import ThermoMetrics
import os
import glob
import sys
from decimal import getcontext, Decimal
import subprocess
import json
import multiprocessing as mp

getcontext().prec = 12

# NOT RUNNING MORPHEUS

class ProcessRawFile:
    """
        Processes a single raw file
        Inserts metric data into database
        Uses SendEmail and Stat
    """
    def __init__(self, file_name, file_path, machine, e_type, filesystem, db_info, venue):

        self.experiment = e_type.upper()
        self.machine = machine
        self.venue = venue
        self.file_name = file_name
        self.fs = filesystem
        self.db = MPMFDBSetUp(db_info["user"], db_info["password"], db_info["database"], self.fs)
        self.raw_file = file_path
        self.metadata = {'filename': self.file_name, 'experiment': self.experiment, 'machine': self.machine, 'loc': self.venue}
        self.outfiles_dir = self.fs.out_dir + "\\" + self.experiment + "\\" + self.machine + "\\" + self.file_name
        self.morph_out_dir = self.outfiles_dir + "\\" + "Morpheus"

        # make and set folder for outfiles
        os.chdir(self.fs.out_dir)
        if not os.path.isdir(self.outfiles_dir):
            os.makedirs(self.experiment + "\\" + self.machine + "\\" + self.file_name)


    # RUN
    def run(self):
        # check if a QC file and not inserted
        check_run = False
        if self.check_file_name():
            if not self.check_run():
                # convert raw file
                if self.run_msconvert():
                    # create xml
                    if self.experiment == "METABOLOMICS":
                        self.create_metab_xml()
                    elif self.experiment == "PROTEOMICS":
                        self.create_proteo_xml()
                    if self.run_mzmine():
                        if self.insert_qc_run_data():
                            if self.experiment == "METABOLOMICS":
                                self.insert_pos_csv()
                                self.insert_neg_csv()
                                self.fwhm_to_seconds()
                                email_data = self.check_email_thresholds_metab()
                                self.insert_summary(email_data)
                                if len(email_data) > 0:
                                    email_data['metadata'] = self.metadata
                                    SendEmail(email_data, self.db, self.fs)
                                else:
                                    print("No Email Sent")
                            elif self.experiment == "PROTEOMICS":
                                self.insert_pos_csv()
                                self.fwhm_to_seconds()
                                if self.run_morpheus(): #FALSE
                                    self.insert_morpheus()
                                    email_data = self.check_email_thresholds_prot()
                                    self.insert_summary(email_data)
                                    if len(email_data) > 0:
                                        email_data['metadata'] = self.metadata
                                        SendEmail(email_data, self.db, self.fs)
                                    else:
                                        print("No Email Sent")
                                else:
                                    print("Morpheus error " + self.file_name)
                            print("Inserted Data for " + self.machine + " " + self.file_name)
                            #self.delete_files()
                            check_run = True
                        else:
                            print("Insert run details error " + self.file_name)
                    else:
                        print("mzMine: processing error " + self.file_name)
                else:
                    print("msconvert: file too small or still writing  " + self.file_name)
            else:
                print("Already Inserted " + self.file_name)
        else:
            print("Incorrect file format " + self.file_name)

        os.chdir(self.fs.main_dir)
        return check_run

    def run_mzmine(self):
        os.chdir(self.fs.sw_dir + "\\" + "MZmine-2.32")
        command = 'startMZmine_Windows.bat ' + '"' + self.outfiles_dir + "\\"+ self.file_name + '.xml' + '"'
        returnvalue = os.system(command)
        if returnvalue:
            return False
        else:
            return True

    def run_mzmine_sub(self):
        # using subprocess if needed
        os.chdir(self.fs.sw_dir + "\\" + "MZmine-2.32")
        p = subprocess.Popen(['startMZmine_Windows.bat',  self.outfiles_dir + "\\" + self.file_name + '.xml'], stdout=subprocess.PIPE)
        p.communicate()
        returnvalue = p.poll()
        # stupid backward logic
        if returnvalue:
            return False
        else:
            return True

    def run_morpheus(self):

        # runs morpheus for thermo files and windows only
        # needs .NET 4.5 or higher and MSFileReader x86
        # NOTE: morpheus uses relative paths, current method for out folder
        #        removes C: (by splicing below) which means the path starts with /
        #           This is a path relative to the current drive root (WATCH for production env)
        # NOTE 4 MULTIPROCESSING: -mt flag sets threads, default will create as many threads as cpus

        morph_db = "HUMAN.fasta"
        morph_dir = self.fs.sw_dir + "\\" + "Morpheus" + "\\" + "morpheus" + "\\" + "thermo"

        if not os.path.isdir(self.morph_out_dir):
            os.makedirs(self.morph_out_dir)
        os.chdir(morph_dir)

        options = {
                '-d': self.raw_file,
                '-o': self.morph_out_dir[2:], # remove splicing for VM ???!!!
                '-db': morph_db,
                '-ad': 'true',
                '-mmu': 'true',
                '-precmtv': '20',
                '-precmtu': 'ppm',
                '-prodmtv': '20',
                '-prodmtu': 'ppm',
                '-pmc': 'true',
                '-minpmo': '-3',
                '-maxpmo': '+1',
                '-vm': 'Ox',
                '-fm': 'AlkC',
                '-acs': 'false'
            }
        #print(options)
        command = 'morpheus_tmo_cl'

        # convert options to string for command line
        option_str = ''
        for key in options:
            option_str += ' %s \'%s\'' % (key, options[key])
        command = command + ' ' + option_str

        # run morpheus
        returnvalue = os.system(command)
        if returnvalue:
            return False
        else:
            return True

    def run_msconvert(self):
        '''Creates .mzXML files in OutFiles'''

        # go to s/w location
        os.chdir(self.fs.sw_dir + "\ProteoWizard")

        # run positive
        command = 'msconvert ' + '"' + self.raw_file + '"' \
                  + ' --filter ' + '"peakPicking true 1-"' + ' --filter ' + '"polarity positive"' \
                  + ' --mzXML -o ' + '"' + self.outfiles_dir + '"' + ' --outfile ' + '"' + self.file_name \
                  + '"' + '_pos'
        returnvalue = os.system(command)
        if returnvalue:
            return False

        if self.experiment == "METABOLOMICS":
            # run negative
            command = 'msconvert ' + '"' + self.raw_file + '"' \
                      + ' --filter ' + '"peakPicking true 1-"' + ' --filter ' + '"polarity negative"' \
                      + ' --mzXML -o ' + '"' + self.outfiles_dir + '"' + ' --outfile ' + '"' + self.file_name \
                      + '"' + '_neg'
            returnvalue = os.system(command)
            if returnvalue:
                return False

        return True

    # CHECK
    def check_run(self):
        # check hasn't already been inserted
        sql = "SELECT * FROM qc_run WHERE file_name = " + "'" + self.file_name + "'"
        try:
            self.db.cursor.execute(sql)
            data = self.db.cursor.fetchall()
        except Exception as e:
            print(e)

        # UPDATE code for proteomics (chroms and pressure)..remove and restore Boolean return b4 next processing
        if len(data) > 0:

            # get run id
            run_id = self.db.get_run_id(self.file_name)

            # delete
            sql = "DELETE from qc_run WHERE run_id = '" + str(run_id) + "'"
            try:
                self.db.cursor.execute(sql)
                self.db.db.commit()
            except Exception as e:
                print(e)

        return False

    def check_file_name(self):
        # QC files must be 23 0r 25 chars in length (year in full or 2 digit)

        # Could limit with lambda function and list comprehesnsion in raw_files array
        # before object is created but this way can identify incorrect files...17,19 for metab files(parkville)
        # ...28,30 for metab clayton
        if len(self.file_name) == 23 or len(self.file_name) == 25 or len(self.file_name) == 17 or len(self.file_name) == 19 or len(self.file_name) == 28 or len(self.file_name) == 30:
            return True
        else:
            return False

    # CREATE
    def create_metab_xml(self):
        pos_file = self.outfiles_dir + "\\" + self.file_name + "_pos.mzXML"
        neg_file = self.outfiles_dir + "\\" + self.file_name + "_neg.mzXML"
        neg_db = self.fs.neg_db
        pos_db = self.fs.pos_db
        batch = self.outfiles_dir + "\\" + self.file_name + ".mzmine"
        outfile_xml = self.outfiles_dir + "\\" + self.file_name + ".xml"
        output_file = self.outfiles_dir
        #print(batch)
        #print(outfile_xml)
        #print(pos_file)
        #print(pos_file)
        #print(neg_db)
        #print(neg_file)

        new_xml = []
        os.chdir(self.fs.main_dir)
        with open(self.fs.xml_template_metab, 'r') as infile:
            for line in infile:
                #print(line)
                new_line = line.strip()
                new_line = new_line.replace('POSINPUTFILE', pos_file)
                new_line = new_line.replace('NEGINPUTFILE', neg_file)
                new_line = new_line.replace('POSDATABASEFILE', pos_db)
                new_line = new_line.replace('NEGDATABASEFILE', neg_db)
                new_line = new_line.replace('DIRECTORY', output_file)
                new_line = new_line.replace('SAMPLEBATCHNAME', batch)
                new_xml.append(new_line)


        with open(outfile_xml, 'w') as outfile:
            for line in new_xml:
                outfile.write(line + "\n")
                #print("XML")
                #print(line)

    def create_proteo_xml(self):
        pos_file = self.outfiles_dir + "\\" + self.file_name + "_pos.mzXML"
        pos_db = self.fs.irt_db
        batch = self.outfiles_dir + "\\" + self.file_name + ".mzmine"
        outfile_xml = self.outfiles_dir + "\\" + self.file_name + ".xml"
        output_file = self.outfiles_dir

        new_xml = []
        os.chdir(self.fs.main_dir)
        with open(self.fs.xml_template_proteo, 'r') as infile:
            for line in infile:
                #print(line)
                new_line = line.strip()
                new_line = new_line.replace('POSINPUTFILE', pos_file)
                new_line = new_line.replace('POSDATABASEFILE', pos_db)
                new_line = new_line.replace('DIRECTORY', output_file)
                new_line = new_line.replace('SAMPLEBATCHNAME', batch)
                new_xml.append(new_line)


        with open(outfile_xml, 'w') as outfile:
            for line in new_xml:
                outfile.write(line + "\n")
                #print("XML")
                #print(line)

    # INSERT
    def insert_morpheus(self):

        # read and insert summary data from morpheus
        with open(self.morph_out_dir + '\\' + 'summary.tsv', 'r') as infile:
            lines = infile.readlines()
        # put summary data in a dict
        summary = {}

        keys = lines[0].split("\t")
        values = lines[1].split("\t")
        for i in range(len(keys)):
            summary[keys[i].strip()] = values[i].strip()

        # get run id
        run_id = self.db.get_run_id(self.file_name)

        # get hela component_id
        sql = "SELECT component_id FROM sample_component WHERE component_name = 'Hela Digest'"
        try:
            self.db.cursor.execute(sql)
            hela_id = self.db.cursor.fetchone()[0]
        except Exception as e:
            print(e)

        for key in summary:
            # get metric_id
            sql = "SELECT metric_id FROM metric WHERE metric_name = '" + key.strip() + "'"
            try:
                self.db.cursor.execute(sql)
                met_id = self.db.cursor.fetchone()
            except Exception as e:
                print(e)

            # insert measurement
            if met_id is not None:
                #print(met_id[0])
                #print(summary[key])
                sql = "INSERT INTO measurement VALUES ( '" + str(met_id[0]) + "','" + str(hela_id) + \
                      "','" + str(run_id) + "','" + str(summary[key]) + "')"

                try:
                    self.db.cursor.execute(sql)
                except Exception as e:
                    print(e)

                self.db.db.commit()

        self.insert_morpheus_ppms(run_id, hela_id)

    def insert_morpheus_ppms(self, rid, hid):

        with open(self.morph_out_dir + '\\' + self.file_name + '.PSMs.tsv','r') as infile:
            lines = infile.readlines()

        # get and remove headers
        headers = lines[0].split('\t')
        lines.pop(0)

        # dict to find index of headers
        index = 0
        indexes = {}
        for header in headers:
            indexes[header.strip()] = index
            index +=1

        # compute average based on constraints
        count = 0
        total = 0
        for line in lines:
            ppm = float(line.split('\t')[indexes['Precursor Mass Error (ppm)']])
            target = line.split('\t')[indexes['Target?']].strip() # in file as capitals but converts to python 'string' bools
            score = float(line.split('\t')[indexes['Morpheus Score']])
            if ppm > -50 and ppm < 50 and target == 'True' and score > 13: # constraints
                total += ppm
                count +=1
                #print(ppm, target, score)

        if count > 0:
            average = total/count
        else:
            average = -1


        # get id for Precursor Mass Error
        sql = "SELECT metric_id FROM metric WHERE metric_name = 'Precursor Mass Error'"
        try:
            self.db.cursor.execute(sql)
            mid = self.db.cursor.fetchone()
        except Exception as e:
            print(e)

        # insert
        sql = "INSERT INTO measurement VALUES ( '" + str(mid[0]) + "','" + str(hid) + \
              "','" + str(rid) + "','" + str(average) + "')"

        try:
            self.db.cursor.execute(sql)
        except Exception as e:
            print(e)

        self.db.db.commit()

    def insert_pos_csv(self):
        # insert pos for v4
        # relies on INSERT ORDER (from DB) REFACTOR?
        # mz, rt, height, area, fwhm, tf, af, min, max, (ppm), (dalton), (areaN), (heightN)
        # change metric names in xml templates and rewrite all insert functions to use names

        run_id = self.db.get_run_id(self.file_name)

        with open(self.outfiles_dir + "\\" +  'posoutput.csv', 'r') as incsv:
            for line in incsv:
                in_data = line.strip().split("|")
                #print(in_data)
                if in_data[0][0] != 'r': # skip first line
                    sql = "SELECT component_id FROM sample_component WHERE component_name = " + "'" + in_data[0] + "'"
                    try:
                        self.db.cursor.execute(sql)
                        comp_id = self.db.cursor.fetchone()
                        #print(comp_id)
                    except Exception as e:
                        print(e)

                    for i in range(1, 10): # watch i as metric id here REFACTOR change metric names to mzmine and get ids
                        # handle nulls, put to 0, other values ??
                        ins_value = str(in_data[i])
                        if ins_value == 'null':
                            ins_value = '0'
                        ins_sql = "INSERT INTO measurement VALUES( " + "'" + str(i) + "', '" + str(comp_id[0]) \
                                  + "', '" + str(run_id) + "', '" + ins_value + "')"
                        try:
                            self.db.cursor.execute(ins_sql)
                            self.db.db.commit()
                        except Exception as e:
                            print(e)

                    self.insert_derived_errors(run_id, comp_id[0])

    def insert_neg_csv(self):
        # insert neg for v4
        # INSERT ORDER (from DB)
        # mz, rt, height, area, fwhm, tf, af, min, max, ppm, dalton, areaN, heightN

        # get run_id
        run_id = self.db.get_run_id(self.file_name)

        with open(self.outfiles_dir + "\\" +  'negoutput.csv', 'r') as incsv:
            for line in incsv:
                in_data = line.strip().split("|")
                #print(in_data)
                if in_data[0][0] != 'r': # skip first line
                    sql = "SELECT component_id FROM sample_component WHERE component_name = " + "'" + in_data[0] + "'"
                    try:
                        self.db.cursor.execute(sql)
                        comp_id = self.db.cursor.fetchone()
                        #print(comp_id)
                    except Exception as e:
                        print(e)

                    for i in range(1, 10):
                        # handle nulls, put to 0, other values ??
                        ins_value = str(in_data[i])
                        if ins_value == 'null':
                            ins_value = '0'
                        ins_sql = "INSERT INTO measurement VALUES( " + "'" + str(i) + "', '" + str(comp_id[0]) \
                                  + "', '" + str(run_id) + "', '" + ins_value + "')"
                        try:
                            self.db.cursor.execute(ins_sql)
                            self.db.db.commit()
                        except Exception as e:
                           print("Database from Process: insert neg csv")

                    self.insert_derived_errors(run_id, comp_id[0])

    def insert_derived_errors(self, r_id, c_id):
        sql = "SELECT exp_mass_charge FROM sample_component WHERE component_id = " + str(c_id)
        try:
            self.db.cursor.execute(sql)
            emc = self.db.cursor.fetchone()
        except Exception as e:
            print(e)


        sql2 = "SELECT value FROM measurement WHERE component_id = " + str(c_id) + " AND run_id = " \
               + str(r_id) + " AND metric_id = 1"
        try:
            self.db.cursor.execute(sql2)
            m_value = self.db.cursor.fetchone()
        except Exception as e:
            print(e)

        # data type nightmare c/o mysql and python
        diff = Decimal(m_value[0]) - Decimal(emc[0])
        ppm = (diff/emc[0]) * Decimal(1e6)
        dalton = diff * Decimal(1e3)
        #print(diff)
        #print(ppm)
        #print(dalton)

        # WATCH: hard coded metric ids based on order from config file
        #        change to selects from db
        ins_sql1 = "INSERT INTO measurement VALUES( " + "'" + "10" + "', '" + str(c_id) \
                                    + "', '" + str(r_id) + "', '" + str(ppm) + "')"

        ins_sql2 = "INSERT INTO measurement VALUES( " + "'" + "11" + "', '" + str(c_id) \
                   + "', '" + str(r_id) + "', '" + str(dalton) + "')"

        try:
            self.db.cursor.execute(ins_sql1)
            self.db.cursor.execute(ins_sql2)
            self.db.db.commit()
        except Exception as e:
            print(e)

    def insert_qc_run_data(self):

        # get id for experiment
        e_sql = "SELECT experiment_id FROM experiment WHERE experiment_type = '" + self.experiment.lower() + "'"

        try:
            self.db.cursor.execute(e_sql)
            eid = self.db.cursor.fetchone()
            self.eid = eid[0] # store for stats
        except Exception as e:
           print(e)

        # get machine id
        m_sql = "SELECT machine_id FROM machine WHERE machine_name = " + "'" + self.machine + "'"

        try:
            self.db.cursor.execute(m_sql)
            mid = self.db.cursor.fetchone()
        except Exception as e:
            print(e)


        run_date = self.get_run_date_time()

        sql = "INSERT INTO qc_run(run_id, file_name, date_time, machine_id, experiment_id, completed) VALUES(NULL,'" \
              + self.file_name + "', CONVERT('" + str(run_date) + "', DATETIME)" + ",'" + str(mid[0]) + \
               "','" + str(self.eid) + "','N'" + ")"
        try:
            self.db.cursor.execute(sql)
        except Exception as e:
            print(e)
            return False

        self.db.db.commit()
        return True

    def insert_summary(self, s_data):

        # get run id
        run_id = self.db.get_run_id(self.file_name)

        # convert to json and update table
        json_data = json.dumps(s_data)
        sql = "UPDATE qc_run SET summary = '" + json_data + "' WHERE run_id = '" + str(run_id) + "'"

        try:
            self.db.cursor.execute(sql)
            self.db.db.commit()
        except Exception as e:
            print(e)

    # EMAIL
    def check_email_thresholds_prot(self):
        # checks metric values against the thresholds in config files
        # and sends email if any outsdide limits

        # get threshold limits
        with open(self.fs.thresh_email) as f:
            limits = f.readlines()

        # remove header
        limits.pop(0)

        # create dict for storing
        thresholds = {}
        for limit in limits:
            new_limit = limit.split("|")
            if new_limit[1] != '':
                thresholds[new_limit[0]] = [new_limit[1], new_limit[2], new_limit[3].strip()]

        # get run_id
        run_id = self.db.get_run_id(self.file_name)

        breaches = {}
        for metric in thresholds:

            # limits
            tot = int(thresholds[metric][0])
            lower = thresholds[metric][1]
            upper = thresholds[metric][2]

            # get metric_id
            sql = "SELECT metric_id FROM metric WHERE metric_name = " + "'" + metric + "'"
            self.db.cursor.execute(sql)
            metric_id = self.db.cursor.fetchone()[0]

            # get values for metric and run_id
            sql = "SELECT c.component_name, v.value FROM " + \
                  "measurement v, sample_component c, metric m " + \
                  "WHERE m.metric_id = v.metric_id AND " + \
                  "c.component_id = v.component_id AND " + \
                  "v.run_id = " + "'" + str(run_id) + "'" + \
                  " AND v.metric_id = " + "'" + str(metric_id) + "'"

            self.db.cursor.execute(sql)
            results = self.db.cursor.fetchall()

            # get components that exceed limits for each metric
            comps = {}
            if metric == "mass_error_ppm":
                # check limits
                for result in results:
                    if result[1] > float(upper) or result[1] < float(lower):
                        comps[result[0]] = [str(round(result[1], 3)) + " ppm"]
                    # catch missed values
                    if result[1] == -1000000.0:
                        comps[result[0]] = ["NO VALUE"]
            elif metric == "area_normalised":
                # check limits
                for result in results:
                    if result[1] < float(lower):
                        comps[result[0]] = [str(round(result[1], 3))]
                    if result[1] == -100.0:
                        comps[result[0]] = ["NO VALUE"]
            elif metric == "fwhm":
                # check limits
                for result in results:
                    if result[1] > float(upper):
                        comps[result[0]] = [str(60 * round(result[1], 3)) + " sec"]
                    if result[1] == 0:
                        comps[result[0]] = ["NO VALUE"]
            elif metric == "tf":
                # check limits
                for result in results:
                    if result[1] > float(upper):
                        comps[result[0]] = [str(round(result[1], 3))]
                    if result[1] == 0:
                        comps[result[0]] = ["NO VALUE"]
            elif metric == "af":
                # check limits
                for result in results:
                    if result[1] > float(upper):
                        comps[result[0]] = [str(round(result[1], 3))]
                    if result[1] == 0:
                        comps[result[0]] = ["NO VALUE"]
            elif metric == "MS/MS Spectra":
                # determine percentiles
                sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                      " ORDER by value"
                self.db.cursor.execute(sql)
                all_results = self.db.cursor.fetchall()
                all_values = [float(item[0]) for item in all_results]

                # get index in ordered list of values
                try:
                    pos = all_values.index(float(results[0][1]))
                    # check upper percentile
                    if (1 - pos / len(all_values)) < float(upper) / 100:
                        comps[metric] = [str(int(results[0][1])),"Top " + str(round((1-pos / len(all_values))*100, 2)) + "%"]
                    # check lower percentile
                    elif pos / len(all_values) < float(lower) / 100:
                        comps[metric] = [str(int(results[0][1])),"Bottom " + str(round((pos / len(all_values))*100, 2)) + "%"]
                except ValueError:
                    pass
            elif metric == "Target PSMs":
                # determine percentiles
                sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                      " ORDER by value"
                self.db.cursor.execute(sql)
                all_results = self.db.cursor.fetchall()
                all_values = [float(item[0]) for item in all_results]

                # get index in ordered list of values
                try:
                    pos = all_values.index(float(results[0][1]))
                    # check lower percentile
                    if pos / len(all_values) < float(lower) / 100:
                        comps[metric] = [str(int(results[0][1])),"Bottom " + str(round((pos / len(all_values))*100, 2)) + "%"]
                except ValueError:
                    pass
            elif metric == "Unique Target Peptides":
                # determine percentiles
                sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                      " ORDER by value"
                self.db.cursor.execute(sql)
                all_results = self.db.cursor.fetchall()
                all_values = [float(item[0]) for item in all_results]

                # get index in ordered list of values
                try:
                    pos = all_values.index(float(results[0][1]))
                    # check lower percentile
                    if pos / len(all_values) < float(lower) / 100:
                        comps[metric] = [str(int(results[0][1])),"Bottom " + str(round((pos / len(all_values))*100, 2)) + "%"]
                except ValueError:
                    pass
            elif metric == "Target Protein Groups":
                # determine percentiles
                sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                      " ORDER by value"
                self.db.cursor.execute(sql)
                all_results = self.db.cursor.fetchall()
                all_values = [float(item[0]) for item in all_results]

                # get index in ordered list of values
                try:
                    pos = all_values.index(float(results[0][1]))
                    # check lower percentile
                    if pos / len(all_values) < float(lower) / 100:
                        comps[metric] = [str(int(results[0][1])),"Bottom " + str(round((pos / len(all_values))*100, 2)) + "%"]
                except ValueError:
                    pass
            elif metric == "Precursor Mass Error":
                # check limits
                for result in results:
                    if result[1] > float(upper) or result[1] < float(lower):
                        comps[metric] = [str(round(result[1], 3)) + " ppm"]
            elif metric == 'rt':
                for result in results:
                    sql = "SELECT component_id FROM sample_component WHERE component_name = " + "'" + str(
                        result[0]) + "'"
                    self.db.cursor.execute(sql)
                    comp_id = self.db.cursor.fetchone()[0]

                    # get all values per component
                    sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                          " AND component_id = " + "'" + str(comp_id) + "'" + " AND value <> 0 " \
                                                                              " ORDER BY value"
                    self.db.cursor.execute(sql)
                    all_results = self.db.cursor.fetchall()
                    all_values = [float(item[0]) for item in all_results]

                    # get index in ordered list of values
                    try:
                        pos = all_values.index(float(result[1]))
                        # check upper percentile
                        if (1 - pos / len(all_values)) < float(upper) / 100:
                            comps[result[0]] = [str(round(result[1], 2)) +" min","Top " + str(
                                round((1 - pos / len(all_values)) * 100, 2)) + "%"]
                        # check lower percentile
                        elif pos / len(all_values) < float(lower) / 100:
                            comps[result[0]] = [str(round(result[1], 2)) + " min" ,"Bottom " + \
                                               str(round((pos / len(all_values)) * 100, 2)) + "%"]
                    except ValueError:
                        pass

                    # catch missed values
                    if result[1] == 0:
                        comps[result[0]] = ["NO VALUE"]

            # add to breaches if tot or more
            if len(comps) >= tot:
                breaches[metric] = comps

        return breaches

    def check_email_thresholds_metab(self):

        # get threshold limits
        with open(self.fs.thresh_email) as f:
            limits = f.readlines()

        # remove header
        limits.pop(0)

        # create dict for storing
        thresholds = {}
        for limit in limits:
            new_limit = limit.split("|")
            if new_limit[1] != '':
                thresholds[new_limit[0]] = [new_limit[1], new_limit[2], new_limit[3].strip()]

        # get run_id
        run_id = self.db.get_run_id(self.file_name)

        breaches = {}
        for metric in thresholds:
            # limits
            tot = int(thresholds[metric][0])
            lower = thresholds[metric][1]
            upper = thresholds[metric][2]

            # get metric_id
            sql = "SELECT metric_id FROM metric WHERE metric_name = " + "'" + metric + "'"
            self.db.cursor.execute(sql)
            metric_id = self.db.cursor.fetchone()[0]

            # get values for metric and run_id (not limited by polarity)
            sql = "SELECT c.component_name, v.value FROM " + \
                  "measurement v, sample_component c, metric m " + \
                  "WHERE m.metric_id = v.metric_id AND " + \
                  "c.component_id = v.component_id AND " + \
                  "v.run_id = " + "'" + str(run_id) + "'" + \
                  " AND v.metric_id = " + "'" + str(metric_id) + "'"

            self.db.cursor.execute(sql)
            results = self.db.cursor.fetchall()

            # get components that exceed limits for each metric
            comps = {}
            if metric == "mass_error_ppm":
                modes = ['N', 'P']
                for mode in modes:
                    # get values by polarity
                    comps = {}
                    sql = "SELECT c.component_name, v.value FROM " + \
                          "measurement v, sample_component c, metric m " + \
                          "WHERE m.metric_id = v.metric_id AND " + \
                          "c.component_id = v.component_id AND " + \
                          "v.run_id = " + "'" + str(run_id) + "'" + \
                          " AND v.metric_id = " + "'" + str(metric_id) + "'" + \
                          " AND c.component_mode =" + "'" + mode + "'"

                    self.db.cursor.execute(sql)
                    results = self.db.cursor.fetchall()

                    # check limits
                    for result in results:
                        if result[1] > float(upper) or result[1] < float(lower):
                            comps[result[0]] = [str(round(result[1], 3)) + " ppm"]

                        # catch missed values
                        if result[1] == -1000000.0:
                            comps[result[0]] = ["NO VALUE"]

                    # add to breaches if tot or more
                    if len(comps) >= tot:
                        if mode == 'N':
                            breaches[metric + "_Neg"] = comps
                        else:
                            breaches[metric + "_Pos"] = comps
            elif metric == 'rt':
                for result in results:
                    sql = "SELECT component_id FROM sample_component WHERE component_name = " + "'" + str(
                        result[0]) + "'"
                    self.db.cursor.execute(sql)
                    comp_id = self.db.cursor.fetchone()[0]

                    # get all values per component
                    sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                          " AND component_id = " + "'" + str(comp_id) + "'" + " AND value <> 0 " \
                                                                              " ORDER BY value"
                    self.db.cursor.execute(sql)
                    all_results = self.db.cursor.fetchall()
                    all_values = [float(item[0]) for item in all_results]

                    # get index in ordered list of values
                    try:
                        pos = all_values.index(float(result[1]))
                        # check upper percentile
                        if (1 - pos / len(all_values)) < float(upper) / 100:
                            comps[result[0]] = [str(round(result[1], 2)) + " min", "Top " + str(round((1-pos / len(all_values))*100, 2)) + "%"]
                        # check lower percentile
                        elif pos / len(all_values) < float(lower) / 100:
                            comps[result[0]] = [str(round(result[1], 2)) + " min", "Bottom " + \
                                               str(round((pos / len(all_values))*100, 2))  + "%"]
                    except ValueError:
                        pass

                    # catch missed values
                    if result[1] == 0:
                        comps[result[0]] =["NO VALUE"]

                if len(comps) >= tot:
                    breaches[metric] = comps
            elif metric == 'area_normalised':
                for result in results:
                    sql = "SELECT component_id FROM sample_component WHERE component_name = " + "'" + str(
                        result[0]) + "'"
                    self.db.cursor.execute(sql)
                    comp_id = self.db.cursor.fetchone()[0]

                    # get all values per component
                    sql = "SELECT value FROM measurement WHERE metric_id = " + "'" + str(metric_id) + "'" + \
                          " AND component_id = " + "'" + str(comp_id) + "'" + " AND value <> -100 " \
                                                                              " ORDER BY value"
                    self.db.cursor.execute(sql)
                    all_results = self.db.cursor.fetchall()
                    all_values = [float(item[0]) for item in all_results]

                    # get index in ordered list of values
                    try:
                        pos = all_values.index(float(result[1]))
                        # check upper percentile
                        if (1 - pos / len(all_values)) < float(upper) / 100:
                            comps[result[0]] = [str(round(result[1], 2)) , "Top " + str(round((1-pos / len(all_values))*100, 2)) + "%"]
                        # check lower percentile
                        elif pos / len(all_values) < float(lower) / 100:
                            comps[result[0]] = [str(round(result[1], 2)) , "Bottom " + str(round((pos / len(all_values))*100, 2)) + "%"]
                    except ValueError:
                        pass

                    # catch missed values
                    if result[1] == -100:
                        comps[result[0]] = ["NO VALUE"]

                if len(comps) >= tot:
                    breaches[metric] = comps

        return breaches

    # OTHER
    def fwhm_to_seconds(self):
        sql = "SELECT metric_id FROM metric WHERE metric_name = 'fwhm'"

        try:
            self.db.cursor.execute(sql)
            fwhm_id = self.db.cursor.fetchone()[0]
        except Exception as e:
            print(e)

        sql = "SELECT run_id FROM qc_run WHERE file_name = " + "'" + self.file_name + "'"

        try:
            self.db.cursor.execute(sql)
            run_id = self.db.cursor.fetchone()[0]
        except Exception as e:
            print(e)

        update_sql = "UPDATE measurement SET value = value*60 WHERE run_id = " + "'" + str(run_id) + "'" + \
                    " AND metric_id = " + "'" + str(fwhm_id) + "'"


        try:
            self.db.cursor.execute(update_sql)
            self.db.db.commit()
            #print("Updated fwhm  for " + str(self.file_name))
        except Exception as e:
            print(e)

    def get_run_date_time(self):
        return self.file_name[-12:]

    def test_mass_error(self):
        # test function not in processing
        sql = "SELECT m.metric_name, c.component_name, r.value FROM metric m, sample_component c," \
              " measurement r WHERE r.metric_id = m.metric_id AND c.component_id = r.component_id AND m.metric_id = 1"
        try:
            self.db.cursor.execute(sql)
            results = self.db.cursor.fetchall()
            for result in results:
                #print(result)
                get_cid = "SELECT exp_mass_charge FROM sample_component WHERE component_name = " + "'"  + result[1] + "'"
                try:
                    self.db.cursor.execute(get_cid)
                    emc = self.db.cursor.fetchone()
                    diff = round((float(result[2]) - emc[0]),6)
                    #print("diff " + str(diff))
                    #print("emc "+ str(emc[0]))
                    #print(diff/emc[0])
                    ppm = (diff/emc[0]) * 1e6
                    dalton = diff * 1e3
                    if ppm > 10.0:
                        #print("PPM " + str(ppm))
                        #print("DALTON " + str(dalton))
                        print(ppm)
                        print(result)
                except Exception as e:
                    print(e)
                    print(result[1])

        except Exception as e:
            print(e)

    def delete_files(self):

        # ADD: removal of .scans from chromatograms
        try:
            os.remove(self.outfiles_dir + "\\" + self.file_name + '.xml')
            os.remove(self.outfiles_dir + "\\" + self.file_name + '_pos.mzXML')
            if self.experiment == 'METABOLOMICS':
                os.remove(self.outfiles_dir + "\\" + self.file_name + '_neg.mzXML')
        except Exception as e:
            print(e)

    # DEV and TESTING
    def get_machines_clay_prot(self):
        sql = "SELECT machine_name from machine WHERE machine_venue = 'CLAYTON'"

        try:
            db.cursor.execute(sql)
            machine_names = db.cursor.fetchall()
        except Exception as e:
            print(e)

        return machine_names

    def get_recent_runs(self, machine):

        # get machine id
        sql = "SELECT machine_id FROM machine WHERE machine_name ='" + machine + "'"

        try:
            db.cursor.execute(sql)
            m_id = db.cursor.fetchone()[0]
        except Exception as e:
            print(e)

        # get last 20 runs
        sql = "SELECT file_name FROM qc_run WHERE machine_id ='" + str(m_id) + "'" + \
                " ORDER BY date_time DESC LIMIT 20"

        try:
            db.cursor.execute(sql)
            files = db.cursor.fetchall()
        except Exception as e:
            print(e)

        return files


if __name__ == "__main__":
    # REFACTOR: config from files (using venue?, plus metab set for one machine)

    # set database details, put in config file
    db_info = {"user": "root", "password": "", "database": "mpmfdb"}
    loc = sys.argv[3].upper()
    experiment_type = sys.argv[4].upper()

    # get file system (x 2) and database objects
    fs = FileSystem(sys.argv[1], "", "", "")
    fs2 = FileSystem(sys.argv[1], sys.argv[2], loc, experiment_type) # used in processing and chrom
    db = MPMFDBSetUp(db_info["user"], db_info["password"], db_info["database"], fs)

    # PROTEOMICS in_dir "\\storage.erc.monash.edu\Shares\R-MNHS-MBPF\Shared\qc_automation"
    # METABOLOMICS in_dir "\\storage.erc.monash.edu\Shares\R-MNHS-MBPF\Shared\Metabolomics\QC_runs"

    # get machine names for venue and experiment type
    machines = {}
    run_check = True
    sql = ''
    machine_names = ()

    if experiment_type.strip() == "METABOLOMICS":
        sql = "SELECT machine_name, machine_type FROM machine WHERE" + " machine_venue = '" + loc.strip() + "' AND use_metab = 'Y'"
    elif experiment_type.strip() == "PROTEOMICS":
        sql = "SELECT machine_name, machine_type FROM machine WHERE" + " machine_venue = '" + loc.strip() + "' AND use_prot = 'Y'"
    else:
        print("Enter metabolomics or proteomics")
        run_check = False

    if run_check:
        try:
            db.cursor.execute(sql)
            machine_names = db.cursor.fetchall()
        except Exception as e:
            print(e)
            print("Could not get machines")
            run_check = False

    # CONFIG: if more machines are added for metabolomics they need to be in machine named folders
    #       and the set-up logic here will need to be modified as per proteomics processing (in REFACTOR)
    # PLUS sorting is MEGA slow?

    # get raw files
    raw_files = []
    if run_check:
        if experiment_type == "METABOLOMICS":
            if loc.upper() == 'CLAYTON':
                raw_files = glob.glob(fs.in_dir + '\\' + 'C1_Clayton' + '\\' + 'QC_Metabolomics_*.raw')
                raw_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)  # sort
            elif loc.upper() == 'PARKVILLE':
                raw_files = glob.glob(fs.in_dir + '\\' + 'C2_Parkville' + '\\' + 'M_QC_*.raw')
                raw_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)  # sort
            else:
                print('Could not get files..')
                run_check = False
            machines[machine_names[0][0]] = [raw_files, machine_names[0][1]]
        elif experiment_type == "PROTEOMICS":
            for machine in machine_names:
                raw_files = glob.glob(fs.in_dir + '\\' + machine[0] + '\\' + 'instrument_data' + '\\' + 'HelaiRT1ul_*.raw')
                raw_files.sort(key=lambda x: os.path.getmtime(x), reverse=True) #sort
                machines[machine[0]] = [raw_files, machine[1]]

    # loop through machines and process raw files
    if run_check:
        for machine in machines:
            print("Found " + str(len(machines[machine][0])) + " files")
            print("Venue " + loc + " Machine " + machine)

            for k in range(len(machines[machine][0])):

                # process only last 10 raw files, remove to process all
                if k >= 1:
                    break

                # get file name
                path_array = machines[machine][0][k].split('\\')
                id = path_array[len(path_array) - 1][:-4] # REMOVE .RAW

                # process raw file for mzmine and morpheus metrics (changed to fs2, test next run)
                qc_run = ProcessRawFile(id, machines[machine][0][k], machine, experiment_type, fs2, db_info, loc)

                # only do thermo metrics, pressure and chroms if successsful metric (mzmine/morpheus) insert
                if qc_run.run():
                    # process instrument metrics for thermo machines
                    if machines[machine][1] == "thermo":
                        ThermoMetrics(machines[machine][0][k], id, experiment_type, db)

                    # extract and add chromatogram data
                    Chromatogram(id, fs2, experiment_type, machine, db)

            # update stats and normalised metrics
            new_stat = Stat(experiment_type, db, machine.strip(),  machines[machine][1])
            new_stat.run()





