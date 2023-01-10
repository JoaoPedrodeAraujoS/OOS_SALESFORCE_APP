'''
# comentado try:
from tkinter import Tk, Label, Frame, Entry, StringVar, Button, Radiobutton
from tkinter import Toplevel, N, S, W, E, FLAT, VERTICAL, PhotoImage
from tkinter import filedialog
from tkinter.ttk import Separator
from tkcalendar import DateEntry
from api import SFApi, read_preferences, save_preferences
from transfer import auto_update_records_from_operators_sheets, download_records_as_sheet, upload_modified_sheet, merge_information_transfer
import pandas as pd
import schedule
# comentado except ModuleNotFoundError: 
import os, os.path
 # comentado    import subprocess
  # comentado   subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r','settings/requirements.txt'])


class Application:
    
    def message(self, master, msgs, space_in=9999):
        message_window = Toplevel(master, background='#e8e8e8')
        message_window.title("STATUS")
        # message_window.geometry("400x150")
        counter = 0
        for i, msg in enumerate(msgs):
            label, desc = msg.split(':')
            Label(message_window, text=label, fg='#145374', background='#e8e8e8', font=[
                  'Calibri', '10']).grid(row=counter, column=0, sticky=W, padx=(20, 10), pady=0)
            Label(message_window, text=desc, fg='#145374', background='#e8e8e8', font=[
                  'Calibri', '12']).grid(row=counter, column=1, sticky=W, padx=(10, 20), pady=0)
            counter += 1
            if (i + 1) % space_in == 0:
                Label(message_window, text='', fg='#145374', background='#e8e8e8', font=[
                    'Calibri', '12']).grid(row=counter, column=0, sticky=W, padx=(10, 20), pady=0)
                Label(message_window, text='', fg='#145374', background='#e8e8e8', font=[
                    'Calibri', '12']).grid(row=counter, column=1, sticky=W, padx=(10, 20), pady=0)
                counter += 1

    def on_focus_out(self, element):
        preferences = read_preferences()
        preferences[element] = getattr(self, element).get()
        save_preferences(preferences)

    def on_left_click(self, element):
        preferences = read_preferences()
        preferences[element] = getattr(self, element).get()
        save_preferences(preferences)

    def _set_default_styles(self):
        default_options = {
            'font': ['Calibri', '14'], 'borderwidth': 0, 'highlightthickness': 0}
        for attr in list(self.__dict__.keys()):
            for key, value in default_options.items():
                try:
                    if not isinstance(getattr(self, attr), StringVar) and not isinstance(getattr(self, attr), str):
                        getattr(self, attr)[key] = value
                except AttributeError:
                    pass

    def _set_styles(self, elements):
        for element, styles in elements.items():
            for attr in list(self.__dict__.keys()):
                if not isinstance(getattr(self, attr), StringVar) and not isinstance(getattr(self, attr), str) and attr.startswith(element):
                    for key, value in styles.items():
                        getattr(self, attr)[key] = value

    def _default_actions(self):
        preferences = read_preferences()
        for attr in list(self.__dict__.keys()):
            obj = getattr(self, attr)
            if getattr(obj, "insert", False):
                obj.delete(0, 'end')
                obj.insert(0, preferences.get(attr, ''))
                obj.bind('<FocusOut>', lambda event,
                         x=attr: self.on_focus_out(x))
            elif isinstance(obj, StringVar):
                obj.set(preferences.get(attr, ''))

    def _define_defaults(self):
        self._set_default_styles()
        self._set_styles({
            'label': {
                'fg': '#145374',
                'background': '#e8e8e8',
                'font': ['Calibri', '10']
            },
            'entry': {
                'width': 30,
                'fg': '#00334e',
                'bd': 5,
                'relief': FLAT
            },
            'label_header': {
                'font': ['Calibri', '18', 'bold'],
                'fg': '#00334e'
            },
            'label_info': {
                'font': ['Calibri', '14']
            },
            'button_menu': {
                'background': '#e8e8e8',
                'relief': FLAT,
                'overrelief': FLAT
            },
            'radio': {
                'background': '#e8e8e8',
                'relief': FLAT,
                'overrelief': FLAT,
                'fg': '#00334e',
                'indicatoron': 0
            },
            'button_download_folder': {
                'bd': '1'
            },
            'button_upload_file': {
                'bd': '1'
            },
            'button_merge_information': {
                'bd': '1'
            },
            'date_entry': {
                'foreground': '#e8e8e8',
                'background': '#00334e',
                'disabledbackground': '#e8e8e8',
                'disabledforeground': '#e8e8e8',
                'headersbackground': '#e8e8e8',
                'weekendbackground': '#fff',
                'weekendforeground': '#555',
                'normalforeground': '#555',
                'othermonthwebackground': '#e8e8e8',
                'othermonthweforeground': '#555',
                'othermonthbackground': '#e8e8e8',
                'othermonthforeground': '#555'
            },
            'entry_interval': {
                'width': 8
            }
        })

    def build_scheduled_task(self):
        frame_st = Frame(background='#e8e8e8')

        def auto_update():
            self.save_entries()
            auto_update_records_from_operators_sheets()
            
        def merge_information():
            merge_information_transfer(self)

        def load_file_klm(self, path, converters=None, dtype=None, sheet_name=None):
            df = pd.read_excel(path, converters=converters, dtype=dtype, sheet_name='OOS')
            df = self._skip_blank_header(df)
            df.columns = [col.strip().lower() for col in df.columns]
            return df
        
        def load_file_klm_pirep(self, path, converters=None, dtype=None, sheet_name=None):
            self.sheets_name(path)
            df = pd.read_excel(path, converters=converters, dtype=dtype, sheet_name='Pirep')
            df = self._skip_blank_header(df)
            df.columns = [col.strip().lower() for col in df.columns]
            return df

        self.label_header_scheduled_task = Label(frame_st,
                                                 text='SCHEDULED UPLOAD')

        self.label_pattern = Label(frame_st, text='FILES START WITH')
        self.entry_pattern = Entry(frame_st)

        self.label_interval = Label(frame_st, text='INTERVAL TIME (HOURS)')
        self.entry_interval = Entry(frame_st)

        self.label_upload_since = Label(frame_st, text='UPLOAD DATA SINCE')
        self.date_entry_upload_since = DateEntry(
            frame_st, date_pattern='dd/mm/yyyy')

        with open('settings/logs.txt', 'r+') as f:
            msgs = f.read().split('\n')

        self.label_last_upload = Label(frame_st, text='LAST UPLOAD: ')
        self.label_info_last_upload = Label(
            frame_st, text='')

        self.button_error_logs = Button(
            frame_st, text="Open Logs", fg='#e8e8e8', background='#00334e', relief=FLAT, command=lambda: self.message(frame_st, msgs, 2))
        self.button_upload = Button(
            frame_st, text="Upload", fg='#00334e', background='#fff', command=auto_update)
        self.button_merge_information = Button(
            frame_st, text='Merge', fg='#e8e8e8', background='#00334e', relief=FLAT,    
                command=merge_information)
        self._define_defaults()

        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_scheduled_task.grid(
            row=0, column=1, pady=(20, 20), padx=20)
        self.label_pattern.grid(
            row=1, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_pattern.grid(
            row=2, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_interval.grid(
            row=3, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_interval.grid(
            row=4, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_upload_since.grid(
            row=5, column=1, pady=(10, 0), padx=20, sticky=W)
        self.date_entry_upload_since.grid(
            row=6, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_last_upload.grid(
            row=7, column=1, pady=(20, 0), padx=20, sticky=W)
        self.label_info_last_upload.grid(
            row=7, column=1, pady=(20, 0), padx=20, sticky=E)

        self.button_error_logs.grid(
            row=8, column=1, pady=(30, 20), padx=20, sticky=W)
        self.button_upload.grid(
            row=8, column=1, pady=(30, 20), padx=20, sticky=E)
        self.button_merge_information.grid(
            row=9, column=1, pady=(30, 20), padx=20, sticky=E)

        return frame_st

    def build_settings(self):
        frame_st = Frame(background='#e8e8e8')

        def connect():
            self.save_entries()
            self.message(frame_st, SFApi().connect())

        self.label_header_config = Label(frame_st, text='SETTINGS')
        self.label_token_security = Label(frame_st, text='SESSION ID')
        self.entry_token_security = Entry(frame_st, show="*")

        self.label_organization = Label(frame_st, text='ORGANIZATION')
        self.variable_radio = StringVar()
        self.radio_production = Radiobutton(
            frame_st, text='PROD', value='prod', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))
        self.radio_qa = Radiobutton(
            frame_st, text='QA', value='qa', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))
        self.radio_sandbox = Radiobutton(
            frame_st, text='SANDBOX', value='dev', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))

        self.button_check_login = Button(
            frame_st, text="Check Login", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=connect)

        self._define_defaults()

        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_config.grid(
            row=0, column=1, pady=(20, 20), padx=20)
        self.label_token_security.grid(
            row=5, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_token_security.grid(
            row=6, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_organization.grid(
            row=7, column=1, pady=(10, 0), padx=20, sticky=W)
        self.radio_production.grid(
            row=8, column=1, pady=(0, 0), padx=(20, 0), ipadx=10, sticky=W)
        self.radio_qa.grid(
            row=8, column=1, pady=(0, 0), padx=(120, 0), ipadx=10, sticky=W)
        self.radio_sandbox.grid(
            row=8, column=1, pady=(0, 0), padx=(200, 0), ipadx=10, sticky=W)

        self.button_check_login.grid(
            row=9, column=1, pady=(30, 20), padx=20, sticky=E)

        return frame_st

    def save_entries(self):
        preferences = read_preferences()
        for attr in list(self.__dict__.keys()):
            obj = getattr(self, attr)
            if isinstance(obj, Entry) or isinstance(obj, DateEntry):
                preferences[attr] = obj.get()
        save_preferences(preferences)

    def build_transfer(self):

        frame_st = Frame(background='#e8e8e8')

        def select_folder():
            folder = filedialog.askdirectory()
            self.entry_download_folder.delete(0, 'end')
            self.entry_download_folder.insert(0, folder)
            self.save_entries()

        def select_file():
            f = filedialog.askopenfilename(title="Select file", filetypes=(("All files", "*.*"),
                                                                           ("Excel Files", "*.xls*"), ("CSV Files", "*.csv")))
            self.entry_upload_file.delete(0, 'end')
            self.entry_upload_file.insert(0, f)
            self.save_entries()

        def upload_file():
            msgs = upload_modified_sheet(self.entry_upload_file.get())
            self.message(frame_st, msgs)
            self.save_entries()

        self.label_header_config = Label(frame_st, text='DATA TRANSFER')

        self.label_reference_date = Label(
            frame_st, text='REFERENCE DATE (Optional)')

        self.label_reference_from = Label(
            frame_st, text='FROM')
        self.date_entry_reference_from = DateEntry(
            frame_st, date_pattern='dd/mm/yyyy')

        self.label_reference_to = Label(frame_st, text='TO')
        self.date_entry_reference_to = DateEntry(
            frame_st, date_pattern='dd/mm/yyyy')

        self.label_download_folder = Label(frame_st, text='FOLDER')
        self.entry_download_folder = Entry(frame_st, text='TEST')
        self.button_download_folder = Button(
            frame_st, text='...', command=select_folder)

        p = read_preferences()
        self.button_download = Button(
            frame_st, text="Download", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=lambda: download_records_as_sheet(self.entry_download_folder.get(),
                                                      self.date_entry_reference_from.get(),
                                                      self.date_entry_reference_to.get()))

        self.label_upload_file = Label(frame_st, text='UPLOAD FILE')
        self.entry_upload_file = Entry(frame_st)
        self.button_upload_file = Button(
            frame_st, text='...', command=select_file)

        self.button_upload = Button(
            frame_st, text="Upload", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=upload_file)

        self._define_defaults()

        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_config.grid(
            row=0, column=1, pady=(20, 20), padx=20)

        self.label_reference_date.grid(
            row=1, column=1, pady=(10, 0), padx=20, sticky=N)

        self.label_reference_from.grid(
            row=2, column=1, pady=(0, 0), padx=20, sticky=W)
        self.label_reference_to.grid(
            row=2, column=1, pady=(0, 0), padx=20, sticky=E)
        self.date_entry_reference_from.grid(
            row=3, column=1, pady=(0, 10), padx=20, sticky=W)
        self.date_entry_reference_to.grid(
            row=3, column=1, pady=(0, 10), padx=20, sticky=E)

        self.label_download_folder.grid(
            row=4, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_download_folder.grid(
            row=5, column=1, pady=(0, 10), padx=20, sticky=W)
        self.button_download_folder.grid(
            row=5, column=1, pady=(0, 10), padx=20, sticky=E)

        self.button_download.grid(
            row=6, column=1, pady=(10, 20), padx=20, sticky=E)

        self.label_upload_file.grid(
            row=7, column=1, pady=(20, 0), padx=20, sticky=W)
        self.entry_upload_file.grid(
            row=8, column=1, pady=(0, 10), padx=20, sticky=W)
        self.button_upload_file.grid(
            row=8, column=1, pady=(0, 10), padx=20, sticky=E)
        self.button_upload.grid(
            row=9, column=1, pady=(10, 20), padx=20, sticky=E)

        return frame_st

    def on_closing(self, master):
        self.save_entries()
        master.destroy()

    def __init__(self,master=None):
        # FRAME BUTTONS
        frame_home = Frame(background='#e8e8e8')
        photo = PhotoImage(file="images/config_icon.png")
        self.button_menu_config = Button(
            frame_home, image=photo)
        self.button_menu_config.image = photo

        photo_scheduled = PhotoImage(file="images/scheduled_icon.png")
        self.button_menu_scheduled = Button(
            frame_home, image=photo_scheduled)
        self.button_menu_scheduled.image = photo_scheduled

        photo_transfer = PhotoImage(file="images/transfer_icon.png")
        self.button_menu_transfer = Button(
            frame_home, image=photo_transfer)
        self.button_menu_transfer.image = photo_transfer

        # command=lambda:
        Separator(frame_home, orient=VERTICAL).grid(
            column=1, row=0, rowspan=3, sticky='ns')

        self.button_menu_scheduled['background'] = '#d8d8d8'

        frame_home.grid(row=0, column=0, rowspan=1, columnspan=1)

        self.button_menu_config.grid(
            row=0, column=0, ipadx=20, ipady=20)
        self.button_menu_scheduled.grid(
            row=1, column=0, ipadx=20, ipady=20)
        self.button_menu_transfer.grid(
            row=2, column=0, ipadx=20, ipady=20)

        frame_transfer = self.build_transfer()
        frame_transfer.grid_forget()
        frame_scheduled = self.build_scheduled_task()
        frame_scheduled.grid_forget()
        frame_settings = self.build_settings()

        self.button_menu_config['command'] = lambda: self.switch_frames(
            frame_settings, [frame_scheduled, frame_transfer])
        self.button_menu_scheduled['command'] = lambda: self.switch_frames(
            frame_scheduled, [frame_settings, frame_transfer])
        self.button_menu_transfer['command'] = lambda: self.switch_frames(
            frame_transfer, [frame_settings, frame_scheduled])

        self._default_actions()
        master.protocol("WM_DELETE_WINDOW",
                        lambda: self.on_closing(master))

    @staticmethod
    def switch_frames(render_frame, other_frames=[]):
        path = '\\flmfs05\\VSS\\suporte_tecnico\\FPR\\2 - PRODUTOS\\6 - SPMR\\#capas\\BACKUP\\EXPORT_OPERINFO.py'
        dirs = os.path.abspath(path)
        for file in dirs:
            for frame in other_frames:
                frame.grid_forget()
            render_frame.focus_set()
            render_frame.grid(row=0, column=1, rowspan=1, columnspan=1)


preferences = read_preferences()
#schedule.every(float(preferences.get('entry_interval', 24))).hours.do(
#    auto_update_records_from_operators_sheets)

root = Tk()
root.configure(background='#e8e8e8')
Application(root)
root.title('SalesForce API')
while True:
    root.update_idletasks()
    root.update()
    schedule.run_pending()
# root.mainloop()
'''
# comentado try:
from tkinter import Tk, Label, Frame, Entry, StringVar, Button, Radiobutton
from tkinter import Toplevel, N, S, W, E, FLAT, VERTICAL, PhotoImage
from tkinter import filedialog
from tkinter.ttk import Separator
from tkcalendar import DateEntry
from api import SFApi, read_preferences, save_preferences
from transfer import auto_update_records_from_operators_sheets, download_records_as_sheet, upload_modified_sheet, merge_information_transfer
import pandas as pd
import schedule
import os, os.path


class Application:
    
    def message(self, master, msgs, space_in=9999):
        message_window = Toplevel(master, background='#e8e8e8')
        message_window.title("STATUS")
        # message_window.geometry("400x150")
        counter = 0
        for i, msg in enumerate(msgs):
            label, desc = msg.split(':')
            Label(message_window, text=label, fg='#145374', background='#e8e8e8', font=[
                  'Calibri', '10']).grid(row=counter, column=0, sticky=W, padx=(20, 10), pady=0)
            Label(message_window, text=desc, fg='#145374', background='#e8e8e8', font=[
                  'Calibri', '12']).grid(row=counter, column=1, sticky=W, padx=(10, 20), pady=0)
            counter += 1
            if (i + 1) % space_in == 0:
                Label(message_window, text='', fg='#145374', background='#e8e8e8', font=[
                    'Calibri', '12']).grid(row=counter, column=0, sticky=W, padx=(10, 20), pady=0)
                Label(message_window, text='', fg='#145374', background='#e8e8e8', font=[
                    'Calibri', '12']).grid(row=counter, column=1, sticky=W, padx=(10, 20), pady=0)
                counter += 1

    def on_focus_out(self, element):
        preferences = read_preferences()
        preferences[element] = getattr(self, element).get()
        save_preferences(preferences)

    def on_left_click(self, element):
        preferences = read_preferences()
        preferences[element] = getattr(self, element).get()
        save_preferences(preferences)

    def _set_default_styles(self):
        default_options = {
            'font': ['Calibri', '14'], 'borderwidth': 0, 'highlightthickness': 0}
        for attr in list(self.__dict__.keys()):
            for key, value in default_options.items():
                try:
                    if not isinstance(getattr(self, attr), StringVar) and not isinstance(getattr(self, attr), str):
                        getattr(self, attr)[key] = value
                except AttributeError:
                    pass

    def _set_styles(self, elements):
        for element, styles in elements.items():
            for attr in list(self.__dict__.keys()):
                if not isinstance(getattr(self, attr), StringVar) and not isinstance(getattr(self, attr), str) and attr.startswith(element):
                    for key, value in styles.items():
                        getattr(self, attr)[key] = value

    def _default_actions(self):
        preferences = read_preferences()
        for attr in list(self.__dict__.keys()):
            obj = getattr(self, attr)
            if getattr(obj, "insert", False):
                obj.delete(0, 'end')
                obj.insert(0, preferences.get(attr, ''))
                obj.bind('<FocusOut>', lambda event,
                         x=attr: self.on_focus_out(x))
            elif isinstance(obj, StringVar):
                obj.set(preferences.get(attr, ''))

    def _define_defaults(self):
        self._set_default_styles()
        self._set_styles({
            'label': {
                'fg': '#145374',
                'background': '#e8e8e8',
                'font': ['Calibri', '10']
            },
            'entry': {
                'width': 30,
                'fg': '#00334e',
                'bd': 5,
                'relief': FLAT
            },
            'label_header': {
                'font': ['Calibri', '18', 'bold'],
                'fg': '#00334e'
            },
            'label_info': {
                'font': ['Calibri', '14']
            },
            'button_menu': {
                'background': '#e8e8e8',
                'relief': FLAT,
                'overrelief': FLAT
            },
            'radio': {
                'background': '#e8e8e8',
                'relief': FLAT,
                'overrelief': FLAT,
                'fg': '#00334e',
                'indicatoron': 0
            },
            'button_download_folder': {
                'bd': '1'
            },
            'button_upload_file': {
                'bd': '1'
            },
            'button_merge_information': {
                'bd': '1'
            },
            'button_fill_files': {
                'bd': '1'
            },
            'date_entry': {
                'foreground': '#e8e8e8',
                'background': '#00334e',
                'disabledbackground': '#e8e8e8',
                'disabledforeground': '#e8e8e8',
                'headersbackground': '#e8e8e8',
                'weekendbackground': '#fff',
                'weekendforeground': '#555',
                'normalforeground': '#555',
                'othermonthwebackground': '#e8e8e8',
                'othermonthweforeground': '#555',
                'othermonthbackground': '#e8e8e8',
                'othermonthforeground': '#555'
            },
            'entry_interval': {
                'width': 8
            }
        })

    def build_scheduled_task(self):
        frame_st = Frame(background='#e8e8e8')

        def auto_update():
            self.save_entries()
            auto_update_records_from_operators_sheets()
            
        def merge_information():
            merge_information_transfer(self)

        def load_file_klm(self, path, converters=None, dtype=None, sheet_name=None):
            df = pd.read_excel(path, converters=converters, dtype=dtype, sheet_name='OOS')
            df = self._skip_blank_header(df)
            df.columns = [col.strip().lower() for col in df.columns]
            return df
        
        def load_file_klm_pirep(self, path, converters=None, dtype=None, sheet_name=None):
            self.sheets_name(path)
            df = pd.read_excel(path, converters=converters, dtype=dtype, sheet_name='Pirep')
            df = self._skip_blank_header(df)
            df.columns = [col.strip().lower() for col in df.columns]
            return df

        self.label_header_scheduled_task = Label(frame_st,
                                                 text='SCHEDULED UPLOAD')

        self.label_pattern = Label(frame_st, text='FILES START WITH')
        self.entry_pattern = Entry(frame_st)
        
        self.label_interval = Label(frame_st, text='INTERVAL TIME (HOURS)')
        self.entry_interval = Entry(frame_st)

        self.label_upload_since = Label(frame_st, text='UPLOAD DATA SINCE')
        self.date_entry_upload_since = DateEntry(
            frame_st, date_pattern='dd/mm/yyyy')

        with open('settings/logs.txt', 'r+') as f:
            msgs = f.read().split('\n')

        self.label_last_upload = Label(frame_st, text='LAST UPLOAD: ')
        self.label_info_last_upload = Label(
            frame_st, text='')

        #BUTTON DEFINITION SCHEDULED TASK
        self.button_error_logs = Button(
            frame_st, text="Open Logs", fg='#e8e8e8', background='#00334e', relief=FLAT, command=lambda: self.message(frame_st, msgs, 2))
        self.button_upload = Button(
            frame_st, text="Upload", fg='#00334e', background='#fff', command=auto_update)
        self.button_merge_information = Button(
            frame_st, text='Merge', fg='#e8e8e8', background='#00334e', relief=FLAT,    
            command=merge_information)
        self._define_defaults()

        #BUTTON GRID DEFINITION
        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_scheduled_task.grid(
            row=0, column=1, pady=(20, 20), padx=20)
        self.label_pattern.grid(
            row=1, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_pattern.grid(
            row=2, column=1, pady=(0, 10), padx=20, sticky=W)
        
        self.label_interval.grid(
            row=5, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_interval.grid(
            row=6, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_upload_since.grid(
            row=7, column=1, pady=(10, 0), padx=20, sticky=W)
        self.date_entry_upload_since.grid(
            row=8, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_last_upload.grid(
            row=9, column=1, pady=(20, 0), padx=20, sticky=W)
        self.label_info_last_upload.grid(
            row=9, column=1, pady=(20, 0), padx=20, sticky=E)

        self.button_error_logs.grid(
            row=10, column=1, pady=(30, 20), padx=20, sticky=W)
        self.button_upload.grid(
            row=10, column=1, pady=(30, 20), padx=20, sticky=E)
        self.button_merge_information.grid(
            row=11, column=1, pady=(30, 20), padx=20, sticky=E)

        return frame_st
    '''
    def build_settings(self):
        frame_st = Frame(background='#e8e8e8')

        def connect():
            self.save_entries()
            self.message(frame_st, SFApi().connect())

        self.label_header_config = Label(frame_st, text='SETTINGS')
        self.label_token_security = Label(frame_st, text='SECURITY TOKEN/SESSION ID')
        self.entry_token_security = Entry(frame_st, show="*")

        self.label_organization = Label(frame_st, text='ORGANIZATION')
        self.variable_radio = StringVar()
        self.radio_production = Radiobutton(
            frame_st, text='PROD', value='prod', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))
        self.radio_qa = Radiobutton(
            frame_st, text='QA', value='qa', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))
        self.radio_sandbox = Radiobutton(
            frame_st, text='SANDBOX', value='dev', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))

        self.button_check_login = Button(
            frame_st, text="Check Login", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=connect)

        self._define_defaults()

        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_config.grid(
            row=0, column=1, pady=(20, 20), padx=20)
        self.label_token_security.grid(
            row=5, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_token_security.grid(
            row=6, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_organization.grid(
            row=7, column=1, pady=(10, 0), padx=20, sticky=W)
        self.radio_production.grid(
            row=8, column=1, pady=(0, 0), padx=(20, 0), ipadx=10, sticky=W)
        self.radio_qa.grid(
            row=8, column=1, pady=(0, 0), padx=(120, 0), ipadx=10, sticky=W)
        self.radio_sandbox.grid(
            row=8, column=1, pady=(0, 0), padx=(200, 0), ipadx=10, sticky=W)

        self.button_check_login.grid(
            row=9, column=1, pady=(30, 20), padx=20, sticky=E)

        return frame_st

    def save_entries(self):
        preferences = read_preferences()
        for attr in list(self.__dict__.keys()):
            obj = getattr(self, attr)
            if isinstance(obj, Entry) or isinstance(obj, DateEntry):
                preferences[attr] = obj.get()
        save_preferences(preferences)
    '''

    def build_settings(self):
        frame_st = Frame(background='#e8e8e8')

        def connect():
            self.save_entries()
            self.message(frame_st, SFApi().connect())

        self.label_username = Label(frame_st, text='USERNAME')
        self.entry_username = Entry(frame_st)

        self.label_password = Label(frame_st, text='PASSWORD')
        self.entry_password = Entry(frame_st, show="*")
        
        self.label_token_security = Label(frame_st, text='TOKEN SECURITY')
        self.entry_token_security = Entry(frame_st, show="*")

        self.label_organization = Label(frame_st, text='ORGANIZATION')
        self.variable_radio = StringVar()
        self.radio_production = Radiobutton(
            frame_st, text='PROD', value='prod', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))
        self.radio_qa = Radiobutton(
            frame_st, text='QA', value='qa', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))
        self.radio_sandbox = Radiobutton(
            frame_st, text='SANDBOX', value='dev', variable=self.variable_radio, command=lambda: self.on_left_click('variable_radio'))

        self.button_check_login = Button(
            frame_st, text="Check Login", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=connect)

        self._define_defaults()

        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_config.grid(
            row=0, column=1, pady=(20, 20), padx=20)
        self.label_username.grid(
            row=1, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_username.grid(
            row=2, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_password.grid(
            row=3, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_password.grid(
            row=4, column=1, pady=(0, 10), padx=20, sticky=W)
        self.label_token_security.grid(
            row=5, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_token_security.grid(
            row=6, column=1, pady=(0, 10), padx=20, sticky=W)

        self.label_organization.grid(
            row=7, column=1, pady=(10, 0), padx=20, sticky=W)
        self.radio_production.grid(
            row=8, column=1, pady=(0, 0), padx=(20, 0), ipadx=10, sticky=W)
        self.radio_qa.grid(
            row=8, column=1, pady=(0, 0), padx=(120, 0), ipadx=10, sticky=W)
        self.radio_sandbox.grid(
            row=8, column=1, pady=(0, 0), padx=(200, 0), ipadx=10, sticky=W)

        self.button_check_login.grid(
            row=9, column=1, pady=(30, 20), padx=20, sticky=E)

        return frame_st

    def save_entries(self):
        preferences = read_preferences()
        for attr in list(self.__dict__.keys()):
            obj = getattr(self, attr)
            if isinstance(obj, Entry) or isinstance(obj, DateEntry):
                preferences[attr] = obj.get()
        save_preferences(preferences)

    def build_transfer(self):

        frame_st = Frame(background='#e8e8e8')

        def select_folder():
            folder = filedialog.askdirectory()
            self.entry_download_folder.delete(0, 'end')
            self.entry_download_folder.insert(0, folder)
            self.save_entries()

        def select_file():
            f = filedialog.askopenfilename(title="Select file", filetypes=(("All files", "*.*"),
                                                                           ("Excel Files", "*.xls*"), ("CSV Files", "*.csv")))
            self.entry_upload_file.delete(0, 'end')
            self.entry_upload_file.insert(0, f)
            self.save_entries()

        def upload_file():
            msgs = upload_modified_sheet(self.entry_upload_file.get())
            self.message(frame_st, msgs)
            self.save_entries()

        self.label_header_config = Label(frame_st, text='DATA TRANSFER')

        self.label_reference_date = Label(
            frame_st, text='REFERENCE DATE (Optional)')

        self.label_reference_from = Label(
            frame_st, text='FROM')
        self.date_entry_reference_from = DateEntry(
            frame_st, date_pattern='dd/mm/yyyy')

        self.label_reference_to = Label(frame_st, text='TO')
        self.date_entry_reference_to = DateEntry(
            frame_st, date_pattern='dd/mm/yyyy')

        self.label_download_folder = Label(frame_st, text='FOLDER')
        self.entry_download_folder = Entry(frame_st, text='TEST')
        self.button_download_folder = Button(
            frame_st, text='...', command=select_folder)

        p = read_preferences()
        self.button_download = Button(
            frame_st, text="Download", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=lambda: download_records_as_sheet(self.entry_download_folder.get(),
                                                      self.date_entry_reference_from.get(),
                                                      self.date_entry_reference_to.get()))

        self.label_upload_file = Label(frame_st, text='UPLOAD FILE')
        self.entry_upload_file = Entry(frame_st)
        self.button_upload_file = Button(
            frame_st, text='...', command=select_file)

        self.button_upload = Button(
            frame_st, text="Upload", fg='#e8e8e8', background='#00334e', relief=FLAT,
            command=upload_file)

        self._define_defaults()

        frame_st.grid(row=0, column=1, rowspan=1, columnspan=1)
        self.label_header_config.grid(
            row=0, column=1, pady=(20, 20), padx=20)

        self.label_reference_date.grid(
            row=1, column=1, pady=(10, 0), padx=20, sticky=N)

        self.label_reference_from.grid(
            row=2, column=1, pady=(0, 0), padx=20, sticky=W)
        self.label_reference_to.grid(
            row=2, column=1, pady=(0, 0), padx=20, sticky=E)
        self.date_entry_reference_from.grid(
            row=3, column=1, pady=(0, 10), padx=20, sticky=W)
        self.date_entry_reference_to.grid(
            row=3, column=1, pady=(0, 10), padx=20, sticky=E)

        self.label_download_folder.grid(
            row=4, column=1, pady=(10, 0), padx=20, sticky=W)
        self.entry_download_folder.grid(
            row=5, column=1, pady=(0, 10), padx=20, sticky=W)
        self.button_download_folder.grid(
            row=5, column=1, pady=(0, 10), padx=20, sticky=E)

        self.button_download.grid(
            row=6, column=1, pady=(10, 20), padx=20, sticky=E)

        self.label_upload_file.grid(
            row=7, column=1, pady=(20, 0), padx=20, sticky=W)
        self.entry_upload_file.grid(
            row=8, column=1, pady=(0, 10), padx=20, sticky=W)
        self.button_upload_file.grid(
            row=8, column=1, pady=(0, 10), padx=20, sticky=E)
        self.button_upload.grid(
            row=9, column=1, pady=(10, 20), padx=20, sticky=E)

        return frame_st

    def on_closing(self, master):
        self.save_entries()
        master.destroy()

    def __init__(self,master=None):
        # FRAME BUTTONS
        frame_home = Frame(background='#e8e8e8')
        photo = PhotoImage(file="images/config_icon.png")
        self.button_menu_config = Button(
            frame_home, image=photo)
        self.button_menu_config.image = photo

        photo_scheduled = PhotoImage(file="images/scheduled_icon.png")
        self.button_menu_scheduled = Button(
            frame_home, image=photo_scheduled)
        self.button_menu_scheduled.image = photo_scheduled

        photo_transfer = PhotoImage(file="images/transfer_icon.png")
        self.button_menu_transfer = Button(
            frame_home, image=photo_transfer)
        self.button_menu_transfer.image = photo_transfer
        
        
        # command=lambda:
        Separator(frame_home, orient=VERTICAL).grid(
            column=1, row=0, rowspan=3, sticky='ns')

        self.button_menu_scheduled['background'] = '#d8d8d8'

        frame_home.grid(row=0, column=0, rowspan=1, columnspan=1)

        self.button_menu_config.grid(
            row=0, column=0, ipadx=20, ipady=20)
        self.button_menu_scheduled.grid(
            row=1, column=0, ipadx=20, ipady=20)
        self.button_menu_transfer.grid(
            row=2, column=0, ipadx=20, ipady=20)

        frame_transfer = self.build_transfer()
        frame_transfer.grid_forget()
        frame_scheduled = self.build_scheduled_task()
        frame_scheduled.grid_forget()
        frame_settings = self.build_settings()

        self.button_menu_config['command'] = lambda: self.switch_frames(
            frame_settings, [frame_scheduled, frame_transfer])
        self.button_menu_scheduled['command'] = lambda: self.switch_frames(
            frame_scheduled, [frame_settings, frame_transfer])
        self.button_menu_transfer['command'] = lambda: self.switch_frames(
            frame_transfer, [frame_settings, frame_scheduled])

        self._default_actions()
        master.protocol("WM_DELETE_WINDOW",
                        lambda: self.on_closing(master))

    @staticmethod
    def switch_frames(render_frame, other_frames=[]):
        path = '\\flmfs05\\VSS\\suporte_tecnico\\FPR\\2 - PRODUTOS\\6 - SPMR\\#capas\\BACKUP\\EXPORT_OPERINFO.py'
        dirs = os.path.abspath(path)
        for file in dirs:
            for frame in other_frames:
                frame.grid_forget()
            render_frame.focus_set()
            render_frame.grid(row=0, column=1, rowspan=1, columnspan=1)


preferences = read_preferences()
#schedule.every(float(preferences.get('entry_interval', 24))).hours.do(
#    auto_update_records_from_operators_sheets)

root = Tk()
root.configure(background='#e8e8e8')
Application(root)
root.title('SalesForce API')
while True:
    root.update_idletasks()
    root.update()
    schedule.run_pending()
# root.mainloop()
