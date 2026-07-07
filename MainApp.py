
import math
import time
from nicegui import Client, ui, app
import pandas as pd
import asyncio
import asyncpg
import os
from nicegui.events import KeyEventArguments
from datetime import datetime, timedelta
from get_credentials import get_credentials
import qrcode
from io import BytesIO
import smtplib
from email.message import EmailMessage
import stripe
from fastapi.responses import RedirectResponse
from flask import Flask, jsonify, redirect, request
import json

# --- GLOBAL DB POOL ---
global_pool = None
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')
PUBLISHABLE_KEY = os.getenv('STRIPE_PUBLISHABLE_KEY')

def footer():
    bg_color = 'bg-gray-200/70'
    with ui.footer(fixed=False).classes(f'w-full {bg_color} py-4 justify-center items-center flex-wrap md:flex-nowrap shadow-sm'):
        with ui.column().classes('items-center'):
            ui.label('© SwimmingRank.org 2025-2026. All rights reserved.').classes('text-gray-600').style('font-size: 15px')
            ui.label('Developed and Maintained by a Swimmer for the Swimming Community.').classes('text-gray-600').style('font-size: 15px')
            ui.link('Please Donate!', '/donate').classes('text-gray-600').style('font-size: 15px')
PAGE_TITLES = {
    '/': 'Swimmer Search',
    '/rankings': 'Rankings',
    '/aboutme': 'About Me',
    '/privacy': 'Privacy Policy',
    '/feedback': 'Feedback',
    '/discussion': 'Discussion',}


WIDTH = 0
HEIGHT = 0

DIVISION_CONFERENCES = {
    'Div I': [c.strip() for c in open('DivI_conferences.txt')],
    'Div II': [c.strip() for c in open('DivII_conferences.txt')],
    'Div III': [c.strip() for c in open('DivIII_conferences.txt')],
}
with open('upcoming_championship_standards.txt') as f:
    STANDARDS = json.loads(f.read())
    f.close()

async def navbar():
    global WIDTH, HEIGHT
    ui.add_head_html('''
        <script>
        function emitSize() {
            emitEvent('resize', {
                width: document.body.offsetWidth,
                height: document.body.offsetHeight,
            });
        }
        window.onload = emitSize;
        window.onresize = emitSize;
        </script>
    ''')
    size = await ui.run_javascript('''
        return {
            width: window.innerWidth,
            height: window.innerHeight
        }
    ''')
    WIDTH = size['width']
    HEIGHT = size['height']
    ui.on('resize', lambda e: get_dim(e))
    ui.on('reload', lambda e: get_dim(e))
    def get_dim(event=None):
        global WIDTH, HEIGHT
        WIDTH = event.args['width']
        HEIGHT = event.args['height']
    if WIDTH < 640:
        with ui.header(elevated=True).classes('''w-full justify-center bg-gray-100 text-gray-800 px-4 border-b border-gray-200 z-50 '''):
            with ui.element('div').classes('w-0 p-0 m-0'):
                menu_btn = ui.button(icon='menu').props('flat round').classes('text-gray-700 absolute left-1/12')
            ui.label('SwimmingRank').classes('font-semibold mx-auto text-[#5898d4]').style('font-size: 2em')
        # ---------- MOBILE FULLSCREEN HEADER ----------
        with ui.dialog(value=False).props('maximized no-backdrop') as mobile_menu:
            with ui.column().classes('h-screen w-full p-6 gap-4 bg-white'):
                # Top bar
                with ui.row().classes('w-full relative items-center'):
                    # CENTERED TITLE (true center)
                    ui.label('Pages').classes('absolute left-1/2 -translate-x-1/2 font-semibold').style('font-size: 1.3rem')

                    # RIGHT: minimize button
                    ui.button(icon='remove', on_click=mobile_menu.close).props('flat round').classes('ml-auto')
                ui.separator()

                # Navigation buttons
                for path, label in PAGE_TITLES.items():
                    ui.button(label, on_click=lambda p=path: (mobile_menu.close(), ui.navigate.to(p))).props('flat').classes(
                        'w-full justify-start px-4 py-3 rounded-lg hover:bg-gray-200').style('font-size: 1.2em')
        menu_btn.on_click(mobile_menu.open)
    else:
        with ui.header(elevated=True, fixed=False).classes('''w-full justify-center bg-gray-100 text-gray-800 px-4 border-b border-gray-200 z-50 '''):
            ui.add_head_html("""
                <style>
                    .q-tab__label {
                        font-size: 1.2rem;
                    }
                </style>
            """)
            with ui.tabs().classes('justify-center') as tabs:
                ui.tab('Swimmer Search').on('click', lambda: ui.navigate.to('/'))
                ui.tab('Rankings').on('click', lambda: ui.navigate.to('/rankings'))
                ui.tab('About Me').on('click', lambda: ui.navigate.to('/aboutme'))
                ui.tab('Privacy Policy').on('click', lambda: ui.navigate.to('/privacy'))
                ui.tab('Feedback').on('click', lambda: ui.navigate.to('/feedback'))
                ui.tab('Discussion').on('click', lambda: ui.navigate.to('/discussion'))

            # Styling for all tabs
            tabs.props('dense').classes('font-bold text-[#5898d4]')

async def get_global_pool():
    """Return a shared asyncpg pool for all requests."""
    dbname, port, password, ip, user = get_credentials()
    global global_pool
    if global_pool is None or global_pool.is_closing():
        global_pool = await asyncpg.create_pool(
            dsn=f'postgres://{user}:{password}@{ip}:{port}/{dbname}', #change to remote address
            max_inactive_connection_lifetime=20,
            min_size=1,
            max_size=10,  # adjust based on server capacity
        )
    return global_pool

def get_current_season():
    session = app.storage.tab
    session['current_month'] = datetime.now().month
    session['current_year'] = datetime.now().year
    if session['current_month'] >= 9:
        session['current_season'] = f"{'9/01/' + str(session['current_year']) + ' - 8/31/' + str(session['current_year'] + 1)}"
    else:
        session['current_season'] = f"{'9/01/' + str(session['current_year'] - 1) + ' - 8/31/' + str(session['current_year'])}"
    return session['current_season']

@app.on_shutdown
async def shutdown():
    """Close global pool on app shutdown."""
    global global_pool
    if global_pool and not global_pool.is_closing():
        await global_pool.close()
    
def get_age_group(age):
    a1 = 0
    a2 = 10
    if age <= 10:
        pass
    elif age <= 12:
        a1, a2 = 11, 12
    elif age <= 14:
        a1, a2 = 13, 14
    elif age <= 16:
        a1, a2 = 15, 16
    elif age <= 18:
        a1, a2 = 17, 18
    else:
        a1, a2 = 19, 99
    return (a1, a2)

def age_group_str(age):
    return f"{age[0]}-{age[1]}"

def convert_timedelta(val):
    minutes = int(val.total_seconds() // 60)
    if minutes == 0:
        seconds = val.total_seconds()
        return f"{round(seconds, 2):.2f}"
    else:
        seconds = val.total_seconds() - 60 * minutes
        return f"{minutes}:{round(seconds, 2):05.2f}"

def str_to_datetime(val):
    if ":" in val:
        date_format = "%M:%S.%f"
    else:
        date_format = "%S.%f"
    return datetime.strptime(val, date_format)

def str_to_timedelta(t_str):
    parts = t_str.split(":")
    if len(parts) == 2:
        minutes = int(parts[0])
        seconds = float(parts[1])
        total_seconds = minutes * 60 + seconds
    else:
        total_seconds = float(parts[0])
    return timedelta(seconds=total_seconds)

def update_conference_options():
    session = app.storage.tab
    session['conference_select'].options = DIVISION_CONFERENCES[session['division_select'].value]
    session['conference_select'].value = None
    session['conference_select'].update()

async def handle_key(e: KeyEventArguments):
    await ui.context.client.connected()
    session = app.storage.tab
    if e.modifiers.ctrl and e.action.keydown:
        session['control_timer'] = time.time()
    elif e.key == 'c' and e.action.keyup:
        if time.time() - session.get('control_timer', 0) < 0.5:
            app.shutdown()  # Stop the NiceGUI application

async def fetch_people(name):
    try:
        name = name.lower().strip().split()
        name[0] = name[0][0].upper() + name[0][1:]
        name[1] = name[1][0].upper() + name[1][1:]
    except:
        name = [name, " "]
    session = app.storage.tab

    pool = await get_global_pool()
    async with pool.acquire() as con:
        query = """
            SELECT "FirstName", "MiddleName", "LastName", "Team", "LSC", "Age", "Sex", "PersonKey"
            FROM "ResultsSchema"."SwimmerIDs"
            WHERE "FirstName" = $1
            AND "LastName"  = $2 AND "Sex" IS NOT NULL"""
        rows = await con.fetch(query, name[0], name[1])

    session['id_table_df'] = pd.DataFrame(rows, columns=['FirstName', 'MiddleName', 'LastName', 'Team', 'LSC', 'Age', 'Sex', 'PersonKey'])
    session['id_table_df'] = session['id_table_df'].rename(columns={'Sex': 'Gender'})
    session['id_table_df'].insert(loc=0, column='Name', value='')
    session['id_table_df']['Name'] = (session['id_table_df'][['FirstName', 'MiddleName', 'LastName']]
                                            .fillna('')                 # replace None/NaN with empty string
                                            .agg(' '.join, axis=1)      # join with spaces
                                            .str.replace(r'\s+', ' ', regex=True)  # collapse double spaces
                                            .str.strip()                # remove leading/trailing spaces
                                        )
    session['id_table_df'].drop(columns=['FirstName', 'MiddleName', 'LastName'], inplace=True)
    await update_id_table()

async def fetch_team_swimmers(team: str):
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            '''
            SELECT *
            FROM "ResultsSchema"."SwimmerIDs"
            WHERE "Team" = $1
            ORDER BY "LastName", "FirstName"
            ''',
            team,
        )

    df = pd.DataFrame([dict(r) for r in rows])
    return df

async def fetch_person_event_data(table, key):
    query = f"""SELECT "Event", "Sex", "SwimTime", "Relay", "Age", "AgeGroup", "Points", "TimeStandard", "LSC", 
                            "Meet", "Team", "SwimDate", "national_rank", "lsc_rank", "team_rank" FROM "ResultsSchema"."{table}" WHERE "PersonKey" = {key}"""
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_ranking_data(table1, table2, age_group, sex, season):
    start_str, end_str = season.split(" - ")
    season_start = datetime.strptime(start_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    season_end = datetime.strptime(end_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    def add_query(table, age_group, sex):
        if table == None:
            return f""""""
        else:
            query = f"""SELECT "Event", "Name", "Sex", "PersonKey", "Age", "LSC", "Team", "SwimTime", "national_rank", "lsc_rank", "team_rank" FROM  "ResultsSchema"."{table}" 
                        WHERE "national_rank" != -1 AND "SwimDate" >= '{season_start}' AND "SwimDate" < '{season_end}' AND "AgeGroup" = '{age_group}' AND "Sex" = {sex} """
            
            return query
    
    query = add_query(table1, age_group, sex)
    if table1 != '100_IM_SCY_results':
        query = query + " UNION ALL " + add_query(table2, age_group, sex)
    query += """ORDER BY "national_rank" ASC"""
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_ncaa_comp_data(time, gender, event):
    if gender == "Male":
        tables = ["DivI_Male",  "DivII_Male",  "DivIII_Male"]
    else:
        tables = ["DivI_Female",  "DivII_Female",  "DivIII_Female"]
    query = f"""SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{tables[0]}"
                WHERE "Event" = '{event}'

                UNION ALL

                SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{tables[1]}"
                WHERE "Event" = '{event}'

                UNION ALL

                SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{tables[2]}"
                WHERE "Event" = '{event}';
                """
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_conference_comp_data(time, gender, event, division, conference):
    table = division.replace(' ', '') + "_"  + gender
    query = f"""SELECT 100.0 * COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM "SwimTime") > {time}) / COUNT(*) AS pct_faster
                FROM "ResultsSchema"."{table}"
                WHERE "Event" = '{event}' AND "ConferenceName" = '{conference}';"""
    
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def fetch_conference_top16_data(gender, event, division, conference):
    table = division.replace(' ', '') + "_"  + gender
    query = f"""SELECT * FROM "ResultsSchema"."{table}" WHERE "ConferenceName" = '{conference}' AND "Event" = '{event}' ORDER BY "SwimTime" LIMIT 16"""
    pool = await get_global_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(query)
    return rows

async def collect_all_event_data(person_key):
    db_table_names = ['50_FR_SCY_results', '50_FR_LCM_results', '100_FR_SCY_results', '100_FR_LCM_results',
                        '200_FR_SCY_results', '200_FR_LCM_results', '400_FR_LCM_results', '500_FR_SCY_results', 
                        '800_FR_LCM_results', '1000_FR_SCY_results', '1500_FR_LCM_results', '1650_FR_SCY_results',
                        '50_BK_SCY_results', '100_BK_SCY_results', '200_BK_SCY_results', '50_BK_LCM_results', '100_BK_LCM_results', '200_BK_LCM_results',
                        '50_FL_SCY_results', '100_FL_SCY_results', '200_FL_SCY_results', '50_FL_LCM_results', '100_FL_LCM_results', '200_FL_LCM_results',
                        '50_BR_SCY_results', '100_BR_SCY_results', '200_BR_SCY_results', '50_BR_LCM_results', '100_BR_LCM_results', '200_BR_LCM_results',
                        '100_IM_SCY_results', '200_IM_SCY_results', '400_IM_SCY_results', '200_IM_LCM_results', '400_IM_LCM_results'
                        ]
    #Person event data
    tasks = []
    for table in db_table_names:
        tasks.append(fetch_person_event_data(table, person_key))
    results = await asyncio.gather(*tasks)
    all_event_data = [item for sublist in results if sublist for item in sublist]
    return all_event_data

async def update_id_table():
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ['Name', 'Team', 'LSC', 'Age', 'Gender']]
    temp = session['id_table_df'].copy()
    def alter_sex(x):
        if x == 0:
            return "Male"
        elif x == 1:
            return "Female"
        else:
            return "None"
    temp['Gender'] = temp['Gender'].apply(lambda x: alter_sex(x))
    session['id_table'].rows = temp.to_dict('records')

    session['id_table'].add_slot('body', r'''
        <q-tr v-if="!props.inGrid" :props="props">
        <q-td key="Name" :props="props" class="big-cell">
            <q-btn
            flat dense color="primary"
            class="big-cell"
            :label="props.row.Name"
            @click="$parent.$emit('person_selected', props.row)"
            />
        </q-td>

        <q-td key="Team" class="big-cell">{{ props.row.Team }}</q-td>
        <q-td key="LSC" class="big-cell">{{ props.row.LSC }}</q-td>
        <q-td key="Age" class="big-cell">{{ props.row.Age }}</q-td>
        <q-td key="Gender" class="big-cell">{{ props.row.Gender }}</q-td>
        </q-tr>
    ''')
    session['id_table'].update()
    session['id_table'].visible = True

    def on_person_selected(msg):
        person = msg.args 
        session['person'] = person
        ui.navigate.to(f'/swimmer/{person["PersonKey"]}')

    session['id_table'].on('person_selected', on_person_selected)

async def update_progression_chart():
    # 1) prepare SCY series
    await ui.context.client.connected()
    session = app.storage.tab
    def get_series(df):
        df_copy = df.copy()
        df_copy['parsed_time'] = df_copy['SwimTime'].apply(lambda x: str_to_timedelta(x.replace("r", "")))
        df_copy['parsed_date'] = pd.to_datetime(df_copy['SwimDate'], format='%m/%d/%Y')
        df_copy.sort_values('parsed_date', inplace=True)

        return df_copy.apply(
            lambda row: [row['parsed_date'].timestamp() * 1000,
                        row['parsed_time'].total_seconds()],
            axis=1
        ).tolist()


    series = []
    if not session['scy_df'].empty:
        scy_series = get_series(session['scy_df'])
        series.append({
                'name': 'SCY',
                'type': 'line',
                'data': scy_series,
                'smooth': False,
                'itemStyle': { 'color': 'blue' },
                'lineStyle': { 'width': 3 },
                'symbolSize': 6,
            })
    else:
        scy_series = []
    if not session['lcm_df'].empty:
        lcm_series = get_series(session['lcm_df'])
        series.append({
                'name': 'LCM',
                'type': 'line',
                'data': lcm_series,
                'smooth': False,
                'itemStyle': { 'color': 'red' },
                'lineStyle': { 'width': 3 },
                'symbolSize': 6,
            })
    else:
        lcm_series = []
    all_times = [point[1] for point in scy_series + lcm_series]
    min_time = min(all_times)
    min_with_buffer = math.floor(min_time * 0.8 / 5) * 5
    
    option = {
        'legend': { 'data': ['SCY', 'LCM'] },
        'tooltip': {
            'trigger': 'axis',
            ':formatter': """function(params) {
                function formatTime(seconds) {
                    const min = Math.floor(seconds / 60);
                    const sec = (seconds % 60).toFixed(2).padStart(5, '0');
                    return min > 0 ? `${min}:${sec}` : sec;
                }
                return params.map(p => 
                    p.seriesName + ' (' + new Date(p.value[0]).toLocaleDateString() + '): ' + formatTime(p.value[1])
                ).join('<br/>');
            }"""
            },
        'xAxis': {
            'type': 'time',
            'name': 'Date',
        },
        'yAxis': {
            'type': 'value',
            'name': 'Time',
            'min': min_with_buffer,
            'axisLabel': {
                ':formatter': """
                    function (value) {
                    const min = Math.floor(value / 60);
                    const sec = (value % 60).toFixed(2).padStart(5, '0');
                    return min + ':' + sec;
                    }"""
            }
        },
        'series': series
    }
    session['chart'].options.clear()
    session['chart'].options.update(option)
    session['chart'].update()
    session['chart'].visible = True

async def update_results_table(course):
    session = app.storage.tab
    cols = ["Event", "SwimTime", "Age", "Points", "TimeStandard", "Meet", "Team", "SwimDate"]
    if course == "SCY":
        session['event_results_table'].columns = [{'name': col, 'label': col, 'field': col} for col in cols]
        session['event_results_table'].rows = session['scy_df'].to_dict('records')
    else:
        session['event_results_table'].columns = [{'name': col, 'label': col, 'field': col} for col in cols]
        session['event_results_table'].rows = session['lcm_df'].to_dict('records')
    session['event_results_table'].visible = True
    session['event_results_table'].update()

async def update_upcoming_meets_table(course):
    session = app.storage.tab
    if session['current_event_graph_page'] == "100 IM":
        session['upcoming_meets_table'].visible = False
        return
    upcoming_meets = STANDARDS.keys()
    rows = []
    age_group = ""
    event_map = {
        '50 FR SCY': '50 FR', '50 FR LCM': '50 FR', '100 FR SCY': '100 FR', '100 FR LCM': '100 FR',
        '200 FR SCY': '200 FR', '200 FR LCM': '200 FR', '400 FR LCM': '400/500 FR', '500 FR SCY': '400/500 FR',
        '800 FR LCM': '800/1000 FR', '1000 FR SCY': '800/1000 FR', '1500 FR LCM': '1500/1650 FR', '1650 FR SCY': '1500/1650 FR',
        '50 BK SCY': '50 BK', '50 BK LCM': '50 BK', '100 BK SCY': '100 BK', '100 BK LCM': '100 BK',
        '200 BK SCY': '200 BK', '200 BK LCM': '200 BK', '50 BR SCY': '50 BR', '50 BR LCM': '50 BR',
        '100 BR SCY': '100 BR', '100 BR LCM': '100 BR', '200 BR SCY': '200 BR', '200 BR LCM': '200 BR',
        '50 FL SCY': '50 FL', '50 FL LCM': '50 FL', '100 FL SCY': '100 FL', '100 FL LCM': '100 FL',
        '200 FL SCY': '200 FL', '200 FL LCM': '200 FL', '200 IM SCY': '200 IM', '200 IM LCM': '200 IM', '400 IM SCY': '400 IM', '400 IM LCM': '400 IM',
    }
    for meet in upcoming_meets:
        standards = STANDARDS[meet].keys()
        if session['person']['Age'] < 19: 
            if '18 & UNDER STANDARDS' in standards:
                age_group = '18 & UNDER STANDARDS'
            else:
                age_group = 'STANDARDS'
        else:
            if '19 & OVER STANDARDS' in standards:
                age_group = '19 & OVER STANDARDS'
            else:
                age_group = 'STANDARDS'
        scy_diff = "NA"
        lcm_diff = "NA"
        if session['person']['Gender'] == "Male":
            scy_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][3]
            if session['current_event_besttime'] != "":
                scy_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime']) - str_to_timedelta(scy_time))
            lcm_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][2]
            if session['current_event_besttime_lcm'] != "":
                lcm_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime_lcm']) - str_to_timedelta(lcm_time))
        else:
            scy_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][0]
            if session['current_event_besttime'] != "":
                scy_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime']) - str_to_timedelta(scy_time))
            lcm_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][1]
            if session['current_event_besttime_lcm'] != "":
                lcm_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime_lcm']) - str_to_timedelta(lcm_time))
        if course == "SCY":
            rows.append({"Meet Name":meet, "Qualifying Time": scy_time, "Course": 'SCY', "Time to Drop": scy_diff})
        else:
            rows.append({"Meet Name":meet, "Qualifying Time": lcm_time, "Course": 'LCM', "Time to Drop": lcm_diff})
        if 'BONUS STANDARDS' in standards:
            age_group = 'BONUS STANDARDS'
            if session['person']['Gender'] == "Male":
                scy_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][3]
                if session['current_event_besttime'] != "":
                    scy_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime']) - str_to_timedelta(scy_time))
                lcm_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][2]
                if session['current_event_besttime_lcm'] != "":
                    lcm_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime_lcm']) - str_to_timedelta(lcm_time))
            else:
                scy_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][0]
                if session['current_event_besttime'] != "":
                    scy_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime']) - str_to_timedelta(scy_time))
                lcm_time = STANDARDS[meet][age_group][event_map[session['current_event_graph_page']]][1]
                if session['current_event_besttime_lcm'] != "":
                    lcm_diff = convert_timedelta(str_to_timedelta(session['current_event_besttime_lcm']) - str_to_timedelta(lcm_time))
            if course == "SCY":
                rows.append({"Meet Name":meet, "Qualifying Time": scy_time, "Course": 'SCY (Bonus)', "Time to Drop": scy_diff})
            else:
                rows.append({"Meet Name":meet, "Qualifying Time": lcm_time, "Course": 'LCM (Bonus)', "Time to Drop": lcm_diff})
        
    session['upcoming_meets_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ["Meet Name", "Qualifying Time", "Course", "Time to Drop"]]
    session['upcoming_meets_table'].rows = rows
    session['upcoming_meets_table'].visible = True
    session['upcoming_meets_table'].update()

async def update_ncaa_comparison_table(event):
    session = app.storage.tab
    if session['current_event_besttime'] == "":
        session['ncaa_comparison_label'].visible = False
        session['ncaa_comparison_table'].visible = False
        return
    session['ncaa_comparison_label'].visible = True
    session['ncaa_comparisons'] = await fetch_ncaa_comp_data(str_to_timedelta(session['current_event_besttime']).total_seconds(), session['person']['Gender'], event)
    session['ncaa_comparisons'] = ["{:.2f}".format(r['pct_faster']) + "%" for r in session['ncaa_comparisons']]
    session['ncaa_comparison_table'].columns = [{'name': col, 'label': "Best Time" if col == "BestTime" else col, 'field': col} for col in ["BestTime", "Division I", "Division II", "Division III"]]
    session['ncaa_comparison_table'].rows = [{'BestTime': session['current_event_besttime'], 'Division I' : session['ncaa_comparisons'][0], 'Division II' : session['ncaa_comparisons'][1], 'Division III' : session['ncaa_comparisons'][2]}]
    session['ncaa_comparison_table'].visible = True
    session['ncaa_comparison_table'].update()

async def add_column_conference_comp_table(e):
    session = app.storage.tab
    conference = session['conference_select'].value
    if not conference:
        return

    if conference in session['selected_conferences']:
        ui.notify('Conference already added')
        return

    if len(session['selected_conferences']) >= 5:
        ui.notify('Maximum of 5 conferences')
        return

    session['selected_conferences'].append(conference)
    try:
        session['conference_div_map']
    except:
        session['conference_div_map'] = {}
    session['conference_div_map'][conference] = session['division_select'].value
    await update_conference_comparison_table(e)

def remove_conference_column(msg):
    session = app.storage.tab
    try:
        i = session['selected_conferences'].index(msg)
    except:
        return
    session['selected_conferences'].pop(i)
    session['conference_div_map'].pop(i)
    session['conference_data'].pop(i)

    columns = [{'name': 'Categories', 'label': 'Categories', 'field': 'Categories'}]
    rows = [{'Categories': 'Percentage Better Than'}, {'Categories': 'Top 8 Time'}, {'Categories': 'Top 16 Time'}]
    for conf in session['selected_conferences']:
        columns.append({'name': conf, 'label': conf, 'field': conf})
        rows[0][conf] = session['conference_data'][conf][0]
        rows[1][conf] = session['conference_data'][conf][1]
        rows[2][conf] = session['conference_data'][conf][2]
    session['conference_comparison_table'].columns = columns
    session['conference_comparison_table'].rows = rows
    session['conference_comparison_table'].visible = True
    session['conference_comparison_table'].update()

async def update_conference_comparison_table(e):
    session = app.storage.tab
    if session['current_event_besttime'] == "":
        session['conference_comparison_label'].visible = False
        session['conference_comparison_table'].visible = False
        session['conference_comparison_row'].visible = False
        return
    session['conference_comparison_label'].visible = True
    session['conference_comparison_row'].visible = True
    for conference in session['selected_conferences']:
        pct = await fetch_conference_comp_data(
            str_to_timedelta(session['current_event_besttime']).total_seconds(),
            session['person']['Gender'],
            e,
            session['conference_div_map'][conference],
            conference,
        )
        top16 = await fetch_conference_top16_data(
            session['person']['Gender'],
            e,
            session['conference_div_map'][conference],
            conference,
        )
        top16time = convert_timedelta(top16[-1]["SwimTime"])
        if len(top16) < 8:
            top8time = top16time
        else:
            top8time = convert_timedelta(top16[7]["SwimTime"])
        session['conference_data'][conference] = ("{:.2f}".format(pct[0]['pct_faster']) + "%", top8time, top16time)

    columns = [{'name': 'Categories', 'label': 'Categories', 'field': 'Categories'}]
    rows = [{'Categories': 'Percentage'}, {'Categories': 'Top 8 Time'}, {'Categories': 'Top 16 Time'}]
    for conf in session['selected_conferences']:
        columns.append({'name': conf, 'label': conf, 'field': conf})
        rows[0][conf] = session['conference_data'][conf][0]
        rows[1][conf] = session['conference_data'][conf][1]
        rows[2][conf] = session['conference_data'][conf][2]
    session['conference_comparison_table'].columns = columns
    session['conference_comparison_table'].rows = rows
    session['conference_comparison_table'].visible = True
    for conf in session['selected_conferences']:
        session['conference_comparison_table'].add_slot(f'header-cell-{conf}', f'''
        <q-th>
            <div class="flex items-center gap-2">
                <span>{conf}</span>
                <q-btn
                    flat dense round icon="close" size="sm"
                    @click="$parent.$emit('remove', '{conf}')"
                />
            </div>
        </q-th>
        ''')

    session['conference_comparison_table'].on('remove', lambda e: remove_conference_column(e.args))
    session['conference_comparison_table'].update()
    
async def update_best_rankings_table():
    await ui.context.client.connected()
    session = app.storage.tab
    session['best_rankings_table'].rows = []
    session['best_rankings_table'].columns = [{'name': col, 'label': col, 'field': col} for col in ["Event", "SwimTime","Age", "Points", "TimeStandard", "Meet", "Team", "SwimDate"]]
    session['current_event_besttime'] = ""
    session['current_event_besttime_lcm'] = ""
    if not session['scy_df'].empty:
        scy_copy = session['scy_df'].copy()
        scy_copy['SwimTime'] = scy_copy['SwimTime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
        scy_min_row = session['scy_df'].loc[scy_copy['SwimTime'].idxmin()].to_dict()  
        session['best_rankings_table'].rows.append(scy_min_row)
        session['current_event_besttime'] = scy_min_row['SwimTime'].strip('r')
    if not session['lcm_df'].empty:
        lcm_copy = session['lcm_df'].copy()
        lcm_copy['SwimTime'] = lcm_copy['SwimTime'].apply(lambda x: str_to_datetime(x.replace('r', "")))
        lcm_min_row = session['lcm_df'].loc[lcm_copy['SwimTime'].idxmin()].to_dict()
        session['best_rankings_table'].rows.append(lcm_min_row)
        session['current_event_besttime_lcm'] = lcm_min_row['SwimTime'].strip('r')
    session['best_rankings_table'].visible = True
    session['best_rankings_table'].update()

async def update_season_rankings_table():
    session = app.storage.tab
    session['season_rankings_table'].columns = [
        {'name': 'Event', 'label': 'Event', 'field': 'Event'},
        {'name': 'SwimDate', 'label': 'Swim Date', 'field': 'SwimDate'},
        {'name': 'AgeGroup', 'label': 'Age Group', 'field': 'AgeGroup'},
        {'name': 'LSC', 'label': 'LSC', 'field': 'LSC'},
        {'name': 'Team', 'label': 'Team', 'field': 'Team'},
        {'name': 'Meet', 'label': 'Meet', 'field': 'Meet'},
        {'name': 'SwimTime', 'label': 'Swim Time', 'field': 'SwimTime'},
        {'name': 'national_rank', 'label': 'National Rank', 'field': 'national_rank'},
        {'name': 'lsc_rank', 'label': 'LSC Rank', 'field': 'lsc_rank'},
        {'name': 'team_rank', 'label': 'Team Rank', 'field': 'team_rank'}
    ]

    session['season_rankings_table'].add_slot('body-cell-national_rank', """
        <q-td :props="props">
            <q-btn flat dense color="primary"
                class="big-cell"
                :label="props.row['national_rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'National', row: props.row})"/>
        </q-td>
    """)

    session['season_rankings_table'].add_slot('body-cell-lsc_rank', """
        <q-td :props="props">
            <q-btn flat dense color="secondary"
                class="big-cell"
                :label="props.row['lsc_rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'LSC', row: props.row})"/>
        </q-td>
    """)

    session['season_rankings_table'].add_slot('body-cell-team_rank', """
        <q-td :props="props">
            <q-btn flat dense color="accent"
                class="big-cell"
                :label="props.row['team_rank']"
                @click="() => $parent.$emit('open_rank_page', {type: 'Team', row: props.row})"/>
        </q-td>
    """)

    session['current_season'] = session['season_rankings_select'].value

    def open_rank_page(msg):
        rank_type = msg.args['type']
        row = msg.args['row']
        ui.navigate.to(f"/rankings?rank_type={rank_type}&event={row['Event']}&age_group={row['AgeGroup']}&lsc={row['LSC']}&team={row['Team']}&sex={int(row['Gender'])}&season={session['current_season']}")
    session['season_rankings_table'].on('open_rank_page', open_rank_page)
    session['season_rankings_table'].rows = []

    start_str, end_str = [s.strip() for s in session['current_season'].split("-")]
    season_start = pd.to_datetime(start_str)
    season_end   = pd.to_datetime(end_str)
    if not session['scy_df'].empty:
        scy_copy = session['scy_df'].copy()
        scy_copy['SwimDate'] = pd.to_datetime(scy_copy['SwimDate'])
        scy_min_season_row = scy_copy[(scy_copy["SwimDate"] >= season_start) & (scy_copy["SwimDate"] <= season_end) & (scy_copy["national_rank"] > 0)]
       
        if not scy_min_season_row.empty:
            scy_min_season_row['SwimDate'] = scy_min_season_row['SwimDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
            scy_min_season_row = scy_min_season_row.to_dict(orient='records')
            session['season_rankings_table'].rows.extend(scy_min_season_row)
    
    if not session['lcm_df'].empty:
        lcm_copy = session['lcm_df'].copy()
        lcm_copy['SwimDate'] = pd.to_datetime(lcm_copy['SwimDate'])
        lcm_min_season_row = lcm_copy[(lcm_copy["SwimDate"] >= season_start) & (lcm_copy["SwimDate"] <= season_end) & (lcm_copy["national_rank"] > 0)]
        
        if not lcm_min_season_row.empty:
            lcm_min_season_row['SwimDate'] = lcm_min_season_row['SwimDate'].apply(lambda x: x.strftime('%m/%d/%Y'))
            lcm_min_season_row = lcm_min_season_row.to_dict(orient='records')
            session['season_rankings_table'].rows.extend(lcm_min_season_row)
    session['season_rankings_table'].visible = True
    session['season_rankings_table'].update()

async def display_event_data(e, df):
    await ui.context.client.connected()
    session = app.storage.tab
    session['lcm_df'] = df.loc[df['Event'].str.contains("LCM")]
    session['scy_df'] = df.loc[df['Event'].str.contains("SCY")]
    session['current_event_graph_page'] = e
    await update_best_rankings_table()
    await update_ncaa_comparison_table(e)
    await update_conference_comparison_table(e)
    await update_upcoming_meets_table(session['upcoming_meets_course_radio'].value)
    await update_season_rankings_table()
    with session['results_column']:
        if session['event_label']:
            session['event_label'].set_text(e + " Progression")
            session['event_label'].update()
    await update_results_table(session['course_radio'].value)
    await update_progression_chart()

async def make_event_buttons(all_event_data_df):
    event_pairs = [('50 FR SCY', '50 FR LCM'), ('100 FR SCY', '100 FR LCM'),
              ('200 FR SCY', '200 FR LCM'), ('500 FR SCY', '400 FR LCM'),
              ('1000 FR SCY', '800 FR LCM'), ('1650 FR SCY', '1500 FR LCM'),
              ('50 FL SCY', '50 FL LCM'), ('100 FL SCY', '100 FL LCM'), ('200 FL SCY', '200 FL LCM'),
              ('50 BR SCY', '50 BR LCM'), ('100 BR SCY', '100 BR LCM'), ('200 BR SCY', '200 BR LCM'),
              ('50 BK SCY', '50 BK LCM'), ('100 BK SCY', '100 BK LCM'), ('200 BK SCY', '200 BK LCM'),
              ('100 IM SCY', '100 IM SCY'), ('200 IM SCY', '200 IM LCM'), ('400 IM SCY', '400 IM LCM')
            ]
    
    first_non_empty_event = None
    first_non_empty_event_df = None
    first = True
    with ui.column().classes('w-full items-center'):
        with ui.row():
            for pair in event_pairs:
                event_df = all_event_data_df.loc[(all_event_data_df['Event'] == pair[0]) | (all_event_data_df['Event'] == pair[1])]
                if not event_df.empty:
                    if first:
                        first_non_empty_event = pair[0]
                        first_non_empty_event_df = event_df
                        first = False
                    ui.button(pair[0].split('SCY')[0], on_click=lambda e=pair[0], df=event_df: display_event_data(e, df)).style('font-size: 1.1rem')

    return (first_non_empty_event, first_non_empty_event_df)

@ui.page('/swimmer/{person_key}')
async def graph_page(person_key: str):
    await ui.context.client.connected()
    session = app.storage.tab
    session['keyboard'] = ui.keyboard(on_key=handle_key)
    name = session['person']['Name']
    age = session['person']['Age']
    lsc = session['person']['LSC']
    try:
        team = session['person']['Team']
    except:
        team = session['person']['Team']
    sex = session['person']['Gender']
    await navbar() 
    ui.add_head_html("""
        <style>
        :root {
            --table-header-font-size: 1.1rem;
            --table-body-font-size: 1rem;
        }

        /* Apply to all tables that use .custom-table */
        .custom-table thead th {
            font-size: var(--table-header-font-size) !important;
            font-weight: 500;
        }

        .custom-table tbody td {
            font-size: var(--table-body-font-size) !important;
        }
                     
        .custom-table big-cell {
            font-size: 1rem;
        }
        .custom-table .big-cell .q-btn__content {
            font-size: 1rem;
        }
        </style>
        """)
    with ui.row().classes('w-full justify-center mt-20 mb-5') as spinnerrow:
        spinner = ui.spinner(size='lg')
    all_event_data = await collect_all_event_data(person_key)
    session['all_event_data_df'] = pd.DataFrame(all_event_data, columns=["Event", "Sex", "SwimTime", "Relay", 
                                                            "Age", "AgeGroup", "Points", "TimeStandard", "LSC",
                                                            "Meet", "Team", "SwimDate", "national_rank", "lsc_rank", "team_rank"])
    session['all_event_data_df'].sort_values(by='SwimDate', inplace=True, ascending=False)
    session['all_event_data_df']["SwimTime"] = session['all_event_data_df'].apply(lambda row: convert_timedelta(row['SwimTime']) + "r" if row['Relay'] == 1 else convert_timedelta(row['SwimTime']), axis=1)
    session['all_event_data_df']["SwimDate"] = session['all_event_data_df']["SwimDate"].apply(lambda x: x.strftime('%m/%d/%Y'))
    session['all_event_data_df'].drop('Relay', axis=1, inplace=True)
    session['all_event_data_df'] = session['all_event_data_df'].rename(columns={'Sex': 'Gender'})
   
    season = get_current_season()
    start_str, end_str = season.split(" - " )
    start_month_day, start_year = start_str.rsplit("/", 1)
    end_month_day, end_year = end_str.rsplit("/", 1)
    start_year = int(start_year)
    end_year = int(end_year)
    all_seasons = [f"{start_month_day}/{start_year - i} - {end_month_day}/{end_year - i}" for i in range(10)]
    
    spinnerrow.delete()
    spinner.delete()
    with ui.column().classes('w-full items-center'):
        ui.label(name).style('font-size: 2rem').classes('font-semibold')
        with ui.grid(columns=2).classes('fit-content gap-0 bg-gray-300'):
            def cell(text, extra_classes=''):
                return ui.label(text).classes(
                    f'bg-white border border-gray-300 p-5 {extra_classes}'
                )
            cell('Team', 'p-1 border-b-0 border-r-0').style('font-size: 1.1rem')
            cell(team,  'text-base font-medium tracking-wide text-primary cursor-pointer hover:bg-gray-100 border-b-0').on('click', lambda e, t=team: ui.navigate.to(f'/team/{t}')).style('font-size: 1.1rem')
            cell('LSC', 'border-b-0 border-r-0').style('font-size: 1.1rem')
            cell(lsc, 'border-b-0').style('font-size: 1.1rem')
            cell('Current Age', 'border-b-0 border-r-0').style('font-size: 1.1rem')
            cell(age, 'border-b-0').style('font-size: 1.1rem')
            cell('Gender', 'border-r-0').style('font-size: 1.1rem')
            cell(sex).style('font-size: 1.1rem')

    first_non_empty_event, first_non_empty_event_df = await make_event_buttons(session['all_event_data_df'])
    session['lcm_df'] = first_non_empty_event_df.loc[first_non_empty_event_df['Event'].str.contains("LCM")]
    session['scy_df'] = first_non_empty_event_df.loc[first_non_empty_event_df['Event'].str.contains("SCY")]
    session['current_event_graph_page'] = first_non_empty_event
    session['best_times_column'] = ui.column().classes('w-full items-center')
    session['ncaa_comparison_column'] = ui.column().classes('w-full items-center')
    session['conference_comparison_column'] = ui.column().classes('w-full items-center')
    session['upcoming_meets_column'] = ui.column().classes('w-full items-center')
    session['season_rankings_column'] = ui.column().classes('w-full items-center')
    session['results_column'] = ui.column().classes('w-full items-center')

    with session['best_times_column']:
        session['best_times_label'] = ui.label('Best Times').style('font-size: 1.6rem')
        with ui.element('div').classes('w-full lg:max-w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
            session['best_rankings_table'] = ui.table(rows=[]).classes('custom-table')
        session['best_rankings_table'].visible = False
    with session['ncaa_comparison_column']:
        session['ncaa_comparison_label'] = ui.label("""NCAA Comparison (Better than % of Swimmers)""").style('font-size: 1.6rem')
        with ui.element('div').classes('w-full lg:max-w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
            session['ncaa_comparison_table'] = ui.table(rows=[]).classes('custom-table')
        session['ncaa_comparison_table'].visible = False
    with session['conference_comparison_column']:
        session['conference_comparison_label'] = ui.label("""Conference Comparison - Add up to 5 conferences""").style('font-size: 1.6rem')
        session['selected_conferences'] = []
        session['conference_data'] = {}
        session['conference_comparison_row'] = ui.row().classes('w-full lg:w-fit p-4 gap-4 bg-gray-100 rounded shadow-sm justify-center items-center')
        with session['conference_comparison_row']:
            session['division_select'] = ui.select(
                    options=['Div I', 'Div II', 'Div III'],
                    value='Div I',
                    label='Division',
                    on_change=lambda e: update_conference_options(),
                ).classes('w-fit sm:min-w-[200px] sm:w-auto').style('font-size: 1.1rem')
            session['conference_select'] = ui.select(
                    options=DIVISION_CONFERENCES['Div I'],
                    label='Conference',
                    with_input=True,
                ).classes('w-fit sm:min-w-[200px]').style('font-size: 1.1rem')
            ui.button(
                    'Add',
                    on_click=lambda: add_column_conference_comp_table(session['current_event_graph_page'])
                ).props('color=primary').classes('w-fit sm:min-w-[200px]').style('font-size: 1.1rem')
        with ui.element('div').classes('w-full lg:max-w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
            session['conference_comparison_table'] = ui.table(rows=[]).classes('custom-table')
        session['conference_comparison_table'].visible = False
    with session['upcoming_meets_column']:
        session['meets_label'] = ui.label('Upcoming Championship Meets').style('font-size: 1.6rem')
        session['upcoming_meets_course_radio'] = ui.radio(["SCY", "LCM"], value="SCY", on_change=lambda: update_upcoming_meets_table(session['upcoming_meets_course_radio'].value)).props('inline').style('font-size: 1.6rem')
        with ui.element('div').classes('w-full lg:max-w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
            session['upcoming_meets_table'] = ui.table(rows=[]).classes('custom-table')
        session['upcoming_meets_table'].visible = False
    with session['season_rankings_column']: 
        with ui.row().classes('w-full justify-center items-center'):
            session['season_rankings_label'] = ui.label('Current Season Rankings: ').style('font-size: 1.6rem')
            session['season_rankings_select'] = ui.select(
                options=all_seasons,
                value=session['current_season'],
                on_change=lambda: update_season_rankings_table()
            ).classes('w-full sm:min-w-[200px] sm:w-auto').style('font-size: 1.3rem')
        with ui.element('div').classes('w-full lg:max-w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
            session['season_rankings_table'] = ui.table(rows=[]).classes('custom-table')
        session['season_rankings_table'].visible = False
    with session['results_column']:
        session['event_label'] = ui.label(first_non_empty_event + " Progression").style('font-size: 1.6rem')
        session['course_radio'] = ui.radio(["SCY", "LCM"], value="SCY", on_change=lambda: update_results_table(session['course_radio'].value)).props('inline').style('font-size: 1.6rem')
        with ui.element('div').classes('w-full lg:max-w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
            session['event_results_table'] = ui.table(rows=[]).classes('custom-table')
        session['event_results_table'].visible = False
        session['chart'] = ui.echart({'series': []}).classes('w-full h-[300px] md:h-[600px]')
        session['chart'].visible = False
    
    footer()
    await display_event_data(first_non_empty_event, first_non_empty_event_df)

@ui.page('/')
async def main_page():
    ui.add_head_html('''
                    <meta name="description" content="SwimmingRank provides rankings, swimmer times, team rankings, and performance analytics across age groups and events.">

                    <meta name="keywords" content="swimming rankings, USA swimming, swimmer rankings, swim times, swim teams, age group swimming">

                    <meta name="robots" content="index, follow">
                    ''')
    await ui.context.client.connected()
    session = app.storage.tab
    session['id_table_df'] = []
    await navbar()
    with ui.column().classes('min-h-screen w-full flex flex-col items-center'):
        session['main_page_column'] = ui.column().classes('w-full flex-1 items-center')

        get_current_season()
        await get_global_pool() 
        with session['main_page_column'].classes('w-full items-center lg:w-4/5'):
            ui.add_head_html("""
                <style>
                    .swimmer-input-class .q-field__native {
                        font-size: 1.1rem;
                    }
                    .my-table th {
                        font-size: 1.1rem;
                    }
                    .my-table .big-cell {
                        font-size: 1rem;
                    }
                    .my-table .big-cell .q-btn__content {
                        font-size: 1rem;
                    }
                </style>
            """)
            ui.html('<h1>Swimmer Search</h1>').style('font-size: 2rem').classes('h-fit font-semibold')
            ui.label('This website provides up-to-date swimming results and rankings data for competitive swimmers in the United States').style('font-size: 1.1rem;')
            ui.label('It contains data for over 1 million swimmers over the past 10 years').style('font-size: 1.1rem')
            ui.label('*NOTICE* USA swimming has changed their data system as you can see if you got to their website which now requires a login to view times and records').style('font-size: 1.1rem;')
            ui.label('This change has rendered my data collection method unviable and I am working to find a new solution. Unfortunately, your most recent results will not be updated in the meantime. Thank you for understanding.').style('font-size: 1.5rem')
            session['search_input'] = ui.input(placeholder='Type a name...').classes('swimmer-input-class')
            session['search_input'].on('keypress.enter', lambda: fetch_people(session['search_input'].value)) 
            with ui.element('div').classes('w-full lg:w-fit overflow-x-auto rounded-md shadow-lg border border-gray-300'):
                session['id_table'] = ui.table(rows=[], columns=[]).classes('my-table')
                session['id_table'].visible = False
            if session['id_table_df'] != []:
                await update_id_table()

    footer()


async def show_page():
    await ui.context.client.connected()
    session = app.storage.tab
    rank_map = {
        'National': 'national_rank',
        'LSC': 'lsc_rank',
        'Team': 'team_rank'
    }

    rank_col = rank_map[session['rank_type_select'].value]

    columns = [
        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
        {'name': 'LSC', 'label': 'LSC', 'field': 'LSC'},
        {'name': 'Team', 'label': 'Team', 'field': 'Team'},
        {'name': 'SwimTime', 'label': 'Time', 'field': 'SwimTime'},
        {'name': rank_col, 'label': 'Rank', 'field': rank_col},
    ]

    session['ranking_table_scy'].columns = columns
    session['ranking_table_scy'].rows = session['current_scy_rank_selection'].to_dict('records')
    session['ranking_table_scy'].visible = not session['current_scy_rank_selection'].empty
    session['ranking_table_scy'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                    class="big-cell" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)

    session['ranking_table_lcm'].columns = columns
    session['ranking_table_lcm'].rows = session['current_lcm_rank_selection'].to_dict('records')
    session['ranking_table_lcm'].visible = not session['current_lcm_rank_selection'].empty
    session['ranking_table_lcm'].add_slot('body-cell-Name', """
        <q-td :props="props">
            <q-btn @click="() => $parent.$emit('person_selected', props.row)"
                    class="big-cell" 
                    :label="props.row.Name" 
                    flat dense color='primary'/>
        </q-td>
    """)

    def on_person_selected(msg):
        person = msg.args  # full row data (Name, Age, etc.)
        # store full info in session (not in URL)
        session['person'] = person
        session['person']['Gender'] = "Male" if session['person']['Gender'] == 0 else "Female"
        # navigate using only the person key
        ui.navigate.to(f'/swimmer/{person["PersonKey"]}')

    session['ranking_table_scy'].on('person_selected', on_person_selected)
    session['ranking_table_lcm'].on('person_selected', on_person_selected)

async def update_page():
    session = app.storage.tab
    rank_map = {
        'National': 'national_rank',
        'LSC': 'lsc_rank',
        'Team': 'team_rank'
    }
    if session['rank_type_select'].value == 'LSC' and session['lsc_select'].value == None:
        return
    if session['rank_type_select'].value == 'Team' and session['team_select'].value == None:
        return
    rank_col = rank_map[session['rank_type_select'].value]

    columns = [
        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
        {'name': 'LSC', 'label': 'LSC', 'field': 'LSC'},
        {'name': 'Team', 'label': 'Team', 'field': 'Team'},
        {'name': 'SwimTime', 'label': 'Time', 'field': 'SwimTime'},
        {'name': rank_col, 'label': 'Rank', 'field': rank_col},
    ]

    session['ranking_table_scy'].columns = columns
    session['ranking_table_scy'].rows = session['current_scy_rank_selection'].to_dict('records')
    
    session['ranking_table_lcm'].columns = columns
    session['ranking_table_lcm'].rows = session['current_lcm_rank_selection'].to_dict('records')
    
    session['ranking_table_scy'].visible = not session['current_scy_rank_selection'].empty
    session['ranking_table_lcm'].visible = not session['current_lcm_rank_selection'].empty

async def refresh_table(event_map):
    await ui.context.client.connected()
    session = app.storage.tab
    session['loading_row'].visible = True
    session['spinner'].visible = True
    sex = 0 if session['sex_select'].value == 'Male' else 1
    ev = session['event_select'].value
    ag = session['age_select'].value
    season = session['season_select'].value
    rt = session['rank_type_select'].value
    ls = session['lsc_select'].value
    cl = session['team_select'].value

    scy_table, lcm_table = event_map[ev + " SCY"]
    rows = await fetch_ranking_data(scy_table, lcm_table, ag, sex, season)
    temp = pd.DataFrame(rows, columns=["Event", "Name", "Sex", "PersonKey", "Age", "LSC", "Team", "SwimTime", "national_rank", "lsc_rank", "team_rank"])
    temp = temp.rename(columns={'Sex':'Gender'})
    temp['SwimTime'] = temp.apply(lambda row: convert_timedelta(row['SwimTime']), axis=1)
    session['scy_ranking_data'] = temp[temp['Event'].str.contains("SCY")]
    session['lcm_ranking_data'] = temp[temp['Event'].str.contains("LCM")]
    
    if rt == 'LSC' and ls:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['LSC'] == ls].drop(columns=['national_rank', 'team_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['LSC'] == ls].drop(columns=['national_rank', 'team_rank'])
    elif rt == 'Team' and cl:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['Team'] == cl].drop(columns=['national_rank', 'lsc_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['Team'] == cl].drop(columns=['national_rank', 'lsc_rank'])
    else:
        session['current_scy_rank_selection'] = session['scy_ranking_data'].drop(columns=['lsc_rank', 'team_rank'])
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'].drop(columns=['lsc_rank', 'team_rank'])
    await update_page()
    session['loading_row'].visible = False
    session['spinner'].visible = False

async def refresh_table_ranksys():
    await ui.context.client.connected()
    session = app.storage.tab
    rt = session['rank_type_select'].value
    ls = session['lsc_select'].value
    cl = session['team_select'].value
    session['loading_row'].visible = True
    session['spinner'].visible = True
    if rt == 'LSC' and ls:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['LSC'] == ls]
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['LSC'] == ls]
    elif rt == 'Team' and cl:
        session['current_scy_rank_selection'] = session['scy_ranking_data'][session['scy_ranking_data']['Team'] == cl]
        session['current_lcm_rank_selection'] = session['lcm_ranking_data'][session['lcm_ranking_data']['Team'] == cl]
    else:
        session['current_scy_rank_selection'] = session['scy_ranking_data']
        session['current_lcm_rank_selection'] = session['lcm_ranking_data']
    await update_page()
    session['loading_row'].visible = False
    session['spinner'].visible = False

@ui.page('/rankings')
async def rankings_page(rank_type: str = 'National', event = '50 FR SCY', age_group = '13-14', lsc = '', team = '', sex: int = 0, season=''):
    await ui.context.client.connected()
    session = app.storage.tab
    ui.add_head_html('''
                    <meta name="description" content=Default rankings page for USA registered swimmers">

                    <meta name="keywords" content="swimming rankings, USA swimming, swimmer rankings, swim times, swim teams, age group swimming">

                    <meta name="robots" content="index, follow">
                    ''')
    ui.add_head_html("""
        <style>
        :root {
            --table-header-font-size: 0.9rem;
            --table-body-font-size: 0.9rem;
        }

        /* Apply to all tables that use .custom-table */
        .custom-table thead th {
            font-size: var(--table-header-font-size) !important;
            font-weight: 500;
        }

        .custom-table tbody td {
            font-size: var(--table-body-font-size) !important;
        }
                     
        .custom-table big-cell {
            font-size: 0.9rem;
        }
        .custom-table .big-cell .q-btn__content {
            font-size: 0.9rem;
        }
        </style>
     """)
    await navbar()
    with ui.column().classes('min-h-screen w-full flex flex-col'):
        event_map = {
            '50 FR SCY'   : ("50_FR_SCY_results",   "50_FR_LCM_results"), '50 FR LCM'   : ("50_FR_SCY_results",   "50_FR_LCM_results"),
            '100 FR SCY'  : ("100_FR_SCY_results",  "100_FR_LCM_results"), '100 FR LCM'  : ("100_FR_SCY_results",  "100_FR_LCM_results"),
            '200 FR SCY'  : ("200_FR_SCY_results",  "200_FR_LCM_results"), '200 FR LCM'  : ("200_FR_SCY_results",  "200_FR_LCM_results"),
            '500 FR SCY'  : ("500_FR_SCY_results",  "400_FR_LCM_results"), '400 FR LCM'  : ("500_FR_SCY_results",  "400_FR_LCM_results"),
            '1000 FR SCY' : ("1000_FR_SCY_results", "800_FR_LCM_results"), '800 FR LCM'  : ("1000_FR_SCY_results", "800_FR_LCM_results"),
            '1650 FR SCY' : ("1650_FR_SCY_results", "1500_FR_LCM_results"), '1500 FR LCM' : ("1650_FR_SCY_results", "1500_FR_LCM_results"),
            '50 FL SCY'   : ("50_FL_SCY_results",   "50_FL_LCM_results"), '50 FL LCM'   : ("50_FL_SCY_results",   "50_FL_LCM_results"),
            '100 FL SCY'  : ("100_FL_SCY_results",  "100_FL_LCM_results"), '100 FL LCM'  : ("100_FL_SCY_results",  "100_FL_LCM_results"),
            '200 FL SCY'  : ("200_FL_SCY_results",  "200_FL_LCM_results"), '200 FL LCM'  : ("200_FL_SCY_results",  "200_FL_LCM_results"),
            '50 BK SCY'   : ("50_BK_SCY_results",   "50_BK_LCM_results"), '50 BK LCM'   : ("50_BK_SCY_results",   "50_BK_LCM_results"),
            '100 BK SCY'  : ("100_BK_SCY_results",  "100_BK_LCM_results"), '100 BK LCM'  : ("100_BK_SCY_results",  "100_BK_LCM_results"),
            '200 BK SCY'  : ("200_BK_SCY_results",  "200_BK_LCM_results"), '200 BK LCM'  : ("200_BK_SCY_results",  "200_BK_LCM_results"),
            '50 BR SCY'   : ("50_BR_SCY_results",   "50_BR_LCM_results"), '50 BR LCM'   : ("50_BR_SCY_results",   "50_BR_LCM_results"),
            '100 BR SCY'  : ("100_BR_SCY_results",  "100_BR_LCM_results"), '100 BR LCM'  : ("100_BR_SCY_results",  "100_BR_LCM_results"),
            '200 BR SCY'  : ("200_BR_SCY_results",  "200_BR_LCM_results"), '200 BR LCM'  : ("200_BR_SCY_results",  "200_BR_LCM_results"),
            '100 IM SCY'  : ("100_IM_SCY_results",  None),
            '200 IM SCY'  : ("200_IM_SCY_results",  "200_IM_LCM_results"), '200 IM LCM'  : ("200_IM_SCY_results",  "200_IM_LCM_results"),
            '400 IM SCY'  : ("400_IM_SCY_results",  "400_IM_LCM_results"), '400 IM LCM'  : ("400_IM_SCY_results",  "400_IM_LCM_results"),}
        
        session['loading_row'] = ui.row().classes('w-full justify-center mt-20 mb-5')
        with session['loading_row']:
            session['spinner'] = ui.spinner(size='lg')
        
        scy_table, lcm_table = event_map[event]
        try:
            season = session['current_season']
        except:
            season = get_current_season()
        if age_group == "10 ":
            age_group = '10 & Under'
        event = event.split('SCY')[0].split('LCM')[0].strip()
        rows = await fetch_ranking_data(scy_table, lcm_table, age_group, sex, season)
        temp = pd.DataFrame(rows, columns=["Event", "Name", "Sex", "PersonKey", "Age", "LSC", "Team", "SwimTime", "national_rank", "lsc_rank", "team_rank"])
        temp = temp.rename(columns={'Sex':'Gender'})
        temp['SwimTime'] = temp.apply(lambda row: convert_timedelta(row['SwimTime']), axis=1)
        session['scy_ranking_data'] = temp[temp['Event'].str.contains("SCY")]
        session['lcm_ranking_data'] = temp[temp['Event'].str.contains("LCM")]
        all_events =['50 FR', '100 FR', '200 FR', '500 FR', '1000 FR', '1650 FR', '50 FL', '100 FL', '200 FL',
                    '50 BK', '100 BK', '200 BK', '50 BR', '100 BR', '200 BR', '100 IM', '200 IM', '400 IM']
        all_sex= ['Male', 'Female']
        all_age_groups = ['10 & Under', '11-12', '13-14', '15-16', '17-18', '19 & Over']
        all_lscs = sorted(temp['LSC'].dropna().unique().tolist())
        all_teams = sorted(temp['Team'].dropna().unique().tolist())

        start_str, end_str = season.split(" - ")
        start_month_day, start_year = start_str.rsplit("/", 1)
        end_month_day, end_year = end_str.rsplit("/", 1)
        start_year = int(start_year)
        end_year = int(end_year)
        all_seasons = [f"{start_month_day}/{start_year - i} - {end_month_day}/{end_year - i}" for i in range(10)]
        with ui.row().classes('w-full justify-center'):  
            ui.html('<h1>Rankings</h1>').style('font-size: 2rem; margin: 0; padding: 0; line-height: 1.2').classes('font-semibold')
        # Dropdowns row
        SELECT_CLASSES = 'w-full sm:min-w-[200px] sm:w-auto'
        with ui.column().classes('w-full gap-4 flex flex-col lg:flex-row items-start justify-center'):
            # ---------------- FILTERS COLUMN ----------------
            with ui.row().classes('w-full lg:w-fit p-4 gap-4 bg-gray-100 rounded shadow-sm justify-center items-center'):
                session['season_select'] = ui.select(
                    options=all_seasons,
                    value=season,
                    label='Season',
                    on_change=lambda: refresh_table(event_map)
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')

                session['sex_select'] = ui.select(
                    options=all_sex,
                    value='Male' if sex == 0 else 'Female',
                    label='Gender',
                    on_change=lambda: refresh_table(event_map)
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')

                session['event_select'] = ui.select(
                    options=all_events,
                    value=event if event in all_events else all_events[0],
                    label='Event',
                    on_change=lambda: refresh_table(event_map)
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')

                session['age_select'] = ui.select(
                    options=all_age_groups,
                    value=age_group if age_group in all_age_groups else all_age_groups[0],
                    label='Age Group',
                    on_change=lambda: refresh_table(event_map)
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')

                session['rank_type_select'] = ui.select(
                    options=['National', 'LSC', 'Team'],
                    value=rank_type,
                    label='Rank Type',
                    on_change=lambda: refresh_table_ranksys()
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')

                session['lsc_select'] = ui.select(
                    options=all_lscs,
                    value=lsc if lsc in all_lscs else None,
                    label='LSC',
                    on_change=lambda: refresh_table_ranksys()
                ).bind_visibility_from(
                    session['rank_type_select'], 'value',
                    backward=lambda v: v == 'LSC'
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')

                session['team_select'] = ui.select(
                    options=all_teams,
                    value=team if team in all_teams else None,
                    label='Team',
                    with_input=True,
                    on_change=lambda: refresh_table_ranksys()
                ).props(
                    'dense outlined clearable'
                ).bind_visibility_from(
                    session['rank_type_select'], 'value',
                    backward=lambda v: v == 'Team'
                ).classes(SELECT_CLASSES).style('font-size: 1.1rem')
            with ui.row().classes('w-full flex flex-col md:flex-col lg:flex-row gap-2 justify-center items-center'):
                with ui.column().classes('w-full lg:w-fit items-center'):
                    ui.label("SCY Rankings").classes('font-semibold').style('font-size: 1.6rem')
                    with ui.element('div').classes('w-full lg:w-fit sm:overflow-x-auto md:overflow-x-auto rounded-md shadow-lg border border-gray-300'):
                        session['ranking_table_scy'] = ui.table(
                            rows=[],
                            columns=[],
                            pagination=25,
                        ).classes('custom-table')

                # ---------------- LCM TABLE ----------------
                with ui.column().classes('w-full lg:w-fit items-center'):
                    ui.label("LCM Rankings").classes('font-semibold').style('font-size: 1.6rem').classes('custom-table').bind_visibility_from(session['event_select'], 'value',backward=lambda v: v != '100 IM')
                    with ui.element('div').classes('w-full lg:w-fit sm:overflow-x-auto rounded-md shadow-lg border border-gray-300'):
                        session['ranking_table_lcm'] = ui.table(
                        rows=[],
                        columns=[],
                        pagination=25
                        ).classes('custom-table').bind_visibility_from(session['event_select'], 'value',backward=lambda v: v != '100 IM')
        
        await refresh_table_ranksys()
        await show_page()
    footer()

@ui.page('/discussion')
async def discussion_page():
    await ui.context.client.connected()
    await navbar()
    with ui.column().classes('min-h-screen w-full flex flex-col'):
        with ui.column().classes('w-full flex-1 items-center py-10 px-6 gap-4'):
            ui.label('Discussion Forum').classes('text-3xl font-bold')

            ui.label('Coming soon 🚧').classes('text-gray-600 text-lg')

            # Placeholder box
            ui.input(placeholder='Start a new topic...').classes('w-1/2')
            ui.button('Post').classes('mt-2')
    footer()

@ui.page('/team/{team}')
async def team_page(team: str):
    await ui.context.client.connected()
    session = app.storage.tab
    await navbar()
    ui.add_head_html("""
                <style>
                    .my-table th {
                        font-size: 1.1rem;
                    }
                    .my-table .big-cell {
                        font-size: 1.1rem;
                    }
                    .my-table .big-cell .q-btn__content {
                        font-size: 1.1rem;
                    }
    """)
    with ui.column().classes('min-h-screen w-full flex flex-col items-center'):
        session['team_df'] = await fetch_team_swimmers(team)

        session['team_df']['Name'] = (
            session['team_df']['FirstName'] + ' ' +
            session['team_df']['MiddleName'].fillna('') + ' ' +
            session['team_df']['LastName']
        ).str.replace('  ', ' ').str.strip()

        age_groups = {
            'All': None,
            '8 & Under': (0, 8),
            '9-10': (9, 10),
            '11-12': (11, 12),
            '13-14': (13, 14),
            '15-18': (15, 18),
            '18+': (18, 200),
        }
        def filter_df():
            group = session['team_age_select'].value
            if age_groups[group] is None:
                return session['team_df']
            low, high = age_groups[group]
            return session['team_df'][(session['team_df']['Age'] >= low) & (session['team_df']['Age'] <= high)]

        def update_tables():
            filtered = filter_df()
            filtered = filtered.rename(columns={'Sex':'Gender'})
            males = filtered[filtered['Gender'] == 0]
            females = filtered[filtered['Gender'] == 1]

            session['team_male_table'].rows = males.to_dict('records')
            session['team_female_table'].rows = females.to_dict('records')

            session['team_male_table'].update()
            session['team_female_table'].update()
        
        def on_person_selected(msg):
            person = msg.args  # full row data (Name, Age, etc.)
            # store full info in session (not in URL)
            session['person'] = person
            session['person']['Gender'] = "Male" if session['person']['Gender'] == 0 else "Female"
            # navigate using only the person key
            ui.navigate.to(f'/swimmer/{person["PersonKey"]}')
            
        with ui.row().classes('w-full justify-center'):
            ui.label(f'Team: {team}').classes('font-semibold mb-4').style('font-size: 2rem')
        with ui.row().classes('gap-4 bg-gray-50 rounded shadow-sm items-center'):
            ui.label('Age Group').classes('text-lg font-semibold').style('font-size: 1.1rem')
            session['team_age_select'] = ui.select( 
            options=list(age_groups.keys()),
            value='All',
            on_change=lambda _: update_tables(),
            ).classes('w-fit-content').style('font-size: 1.1em')
        with ui.row().classes('w-full lg:w-4/5 flex-col md:flex-col lg:flex-row gap-2 justify-center items-center'):
            with ui.column().classes('w-full lg:w-fit items-center'):
                ui.label('Male').classes('font-semibold').style('font-size: 1.5rem')
                with ui.element('div').classes('w-full lg:w-fit sm:overflow-x-auto rounded-md shadow-lg border border-gray-300'):
                    session['team_male_table'] = ui.table(
                    columns=[
                        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
                        {'name': 'Age', 'label': 'Age', 'field': 'Age'},
                    ],
                    rows=[],
                    pagination=20).classes('w-full my-table').style('font-size: 1.1rem')
            
            with ui.column().classes('w-full lg:w-fit items-center'):
                ui.label('Female').classes('font-semibold').style('font-size: 1.5rem')
                with ui.element('div').classes('w-full lg:w-fit sm:overflow-x-auto rounded-md shadow-lg border border-gray-300'):
                    session['team_female_table'] = ui.table(
                    columns=[
                        {'name': 'Name', 'label': 'Name', 'field': 'Name'},
                        {'name': 'Age', 'label': 'Age', 'field': 'Age'},
                    ],
                    rows=[], 
                    pagination=20).classes('w-full my-table').style('font-size: 1.1rem')
        update_tables()
        session['team_female_table'].add_slot('body-cell-Name', """
            <q-td :props="props">
                <q-btn @click="() => $parent.$emit('person_selected', props.row)" 
                        class="big-cell"
                        :label="props.row.Name" 
                        flat dense color='primary'/>
            </q-td>
        """)
        session['team_female_table'].on('person_selected', on_person_selected)

        session['team_male_table'].add_slot('body-cell-Name', """
            <q-td :props="props">
                <q-btn @click="() => $parent.$emit('person_selected', props.row)"
                        class="big-cell" 
                        :label="props.row.Name" 
                        flat dense color='primary'/>
            </q-td>
        """)
        session['team_male_table'].on('person_selected', on_person_selected)
    footer()

@ui.page('/aboutme')
async def aboutme_page():
    await ui.context.client.connected()
    session = app.storage.tab
    await navbar()
    with ui.column().classes('min-h-screen w-full flex flex-col'):
        
        with ui.row().classes('w-full justify-center'):
            with ui.row().classes('w-full lg:w-3/5 justify-center'):
                ui.label('About Me').style('font-size: 2rem').classes('font-semibold')
            with ui.column().classes('w-full lg:w-3/5 items-start'):
                ui.label(
                    "I am a college student and a competitive swimmer. During my swimming career, I used the swimmingrank.com website frequently to check my rankings"
                    " and see how I compared to other swimmers in my age group and events. Like many of my fellow swimmers, I liked the comprehensive information and"
                    " clean design of that website. However, with that website no longer available, I decided to create SwimmingRank.org to fill that gap and provide "
                    "swimmers with a similar resource to track their rankings and progressions over years. My website allows for the search of a particular swimmer, "
                    "provides a comprehensive review of  the meets that swimmer has attended and historical times in any particular event, and tabulates the rankings "
                    "across teams/state/national levels.").style('font-size: 1.1rem')

                
                with ui.row().classes('items-start gap-1'):
                    ui.html('''
                        <p style="font-size:1.1rem; margin: 0 auto;">
                            I will keep improving this website such that it provides the data and information that the
                            swimming community needs. Any comments and suggestions will be greatly appreciated.
                            You can email me directly at
                            <a href="mailto:support@swimmingrank.org" class="text-blue-600 hover:underline">
                                support@swimmingrank.org
                            </a>
                            or provide feedback anonymously on the
                            <a href="/feedback" class="text-blue-600 hover:underline">
                                Feedback Page
                            </a>.
                        </p>
                        ''')
                with ui.row().classes('items-start gap-1'):
                    ui.html('''
                        <p style="font-size:1.1rem;">
                            Finally, it does cost money to run the website and database, so if you would like to
                            support the site please consider donating via the
                            <a href="/donate" class="text-blue-600 hover:underline">Donate page</a>.
                            Thank you!
                        </p>
                        ''')
    footer()

@ui.page('/privacy')
async def privacypolicy_page():
    await ui.context.client.connected()
    await navbar()
    with ui.column().classes('min-h-screen w-full flex flex-col'):
        with ui.row().classes('w-full justify-center items-start'):
            with ui.row().classes('w-full lg:w-3/5 justify-center'):
                ui.label('Privacy Policy').style('font-size: 2rem').classes('font-semibold')
            with ui.column().classes('w-full lg:w-3/5 items-start text-start'):
                ui.label("""SwimmingRank.org is designed to be as privacy friendly as possible. I do not track, collect, or store any of your activities on the site, nor do I use any third-party trackers or ads.
                         All of the data available on this website is publicly available via USA Swimming. I update this website several days a week with meet results.
                         Only meets registered with USA Swimming will be included in the rankings and results, so regular high school dual meets or college
                         meets may not be included.""").style('font-size: 18px')
                ui.label("""Last updated: May 18th, 2025""").style('font-size: 1.1rem')
            """with ui.column().classes('w-3/5 items-center text-center'):
                ui.label('FAQ').style('font-size: 28px')
            with ui.column().classes('w-3/5'):
                ui.label('1. How often is the data updated?').style('font-size: 15px').classes('font-semibold')
                ui.label('The data is updated weekly, typically on Mondays, to include the previous week's meet results.').style('font-size: 15px')
                ui.label('2. Where does the data come from?').style('font-size: 15px').classes('font-semibold')
                ui.label('All data is sourced from publicly available information on USA Swimming's website.').style('font-size: 15px')
                ui.label('3. Why are some meets or times missing?').style('font-size: 15px').classes('font-semibold')
                ui.label('Only meets that are officially registered with USA Swimming are included in the rankings and results. Regular high school duel meets or college meets may not be included. Additionally I have
                            only collected data up to 2016 so results from before that year will not be displayed.').style('font-size: 15px')
            """
    footer()

def create_intent(amount):
    return stripe.PaymentIntent.create(
        amount=amount * 100,
        currency='usd',
        automatic_payment_methods={'enabled': True},
    )

@ui.page('/donate')
async def donate_page():
    await ui.context.client.connected()
    await navbar()

    with ui.row().classes('justify-between w-full p-8 gap-6'):
        with ui.card().classes('gap-4 w-fit items-center'):
            ui.label('Support the Project!').classes('font-bold').style('font-size: 2rem')
            ui.label("Running this site isn't free, and donations help cover things like hosting, services, \
                     and the time it takes to keep everything working smoothly and stay ad/subscription free. Your support helps me \
                     maintain the site, fix bugs, and roll out new features over time. As a college student, even a small \
                     contribution goes a long way and is genuinely appreciated!").style('font-size: 1.2rem')
        with ui.card().classes('gap-4 w-full items-center'):
            with ui.row().classes('w-full justify-between'):
                for amt in [1, 5, 10, 20]:
                    ui.button(
                        f'${amt}',
                        on_click=lambda a=amt: set_amount_buttons(a),
                    ).classes('flex-1')

            custom = ui.number(
                label='Custom Amount',
                on_change=lambda e: set_amount(int(e.value)),
                min=1
            ).props('prefix: $').style('font-size: 1.2rem')

            ui.separator()

            ui.element('div').props("id='payment-element'").classes('w-full')
            ui.label('').props("id='stripe-error'").classes('text-red-500')
            ui.label('').props("id='stripe-success'").classes('text-green-600')

            donate_btn = ui.button('Donate', color='primary').classes('w-full')

    # ---------- DEFINE ALL JS ONCE ----------
    ui.run_javascript(f'''
        window.ensureStripe = function(callback) {{
            if (window.Stripe) {{
                callback();
                return;
            }}
            const s = document.createElement('script');
            s.src = 'https://js.stripe.com/v3/';
            s.onload = callback;
            document.head.appendChild(s);
        }}

        window.mountStripe = function(clientSecret) {{
            if (!window.stripe) {{
                window.stripe = Stripe("{PUBLISHABLE_KEY}");
            }}

            if (window.elements) {{
                document.getElementById("payment-element").innerHTML = "";
            }}

            window.elements = stripe.elements({{ clientSecret }});
            const pe = elements.create("payment");
            pe.mount("#payment-element");
        }}

        window.submitStripePayment = async function() {{
            stripe.confirmPayment({{
                elements,
                redirect: 'if_required',
            }}).then(function(result) {{
                if (result.error) {{
                    document.getElementById("stripe-success").innerText = '';
                    document.getElementById("stripe-error").innerText = result.error.message;
                    }} 
                else {{
                    document.getElementById("stripe-success").innerText = "Donation successful! Thank you for your support.";
                    document.getElementById("stripe-error").innerText = '';}}
                }});
        }}
    ''')
    def set_amount_buttons(amount):
        custom.value = amount
        set_amount(amount)
    # ---------- PYTHON → JS BRIDGE ----------
    def set_amount(amount=5):
        intent = create_intent(amount)
        ui.run_javascript(f'''
            ensureStripe(() => mountStripe("{intent.client_secret}"));
        ''')

    donate_btn.on('click', lambda: ui.run_javascript('submitStripePayment()'))

    set_amount()
    footer()

@ui.page('/thank-you')
def thank_you():
    ui.label('Thank you for your donation!').classes('text-2xl p-8')

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

WEBSITE_EMAIL = "alphadjw@gmail.com"  
EMAIL_PASSWORD = "efpe ptjd sode zdyc"  

def send_feedback_email(message: str, user_email: str, category: str):
    email = EmailMessage()
    email["From"] = WEBSITE_EMAIL
    email["To"] = WEBSITE_EMAIL
    email["Subject"] = f"SwimmingRank: {category}"

    body = f"""
        Message: {message}
        User email: {user_email or "Not provided"} """

    email.set_content(body)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(WEBSITE_EMAIL, EMAIL_PASSWORD)
        server.send_message(email)

@ui.page('/feedback')
async def feedback_page():
    await ui.context.client.connected()
    await navbar()
    with ui.column().classes('min-h-screen w-full flex flex-row'):
        with ui.column().classes('w-full max-w-xl mx-auto p-6 gap-4 bg-white rounded-lg shadow-md items-center'):
            ui.label('Feedback').classes('font-bold text-center').style('font-size: 2rem')
            ui.label('Have a bug, suggestion, or question? Send it below.').classes('text-gray-600 text-center').style('font-size: 1.1rem')

            email_input = ui.input(
                label='Your Email (Optional)',
                placeholder='youremail@example.com'
            ).classes('w-full').props('type=email outlined').style('font-size: 1.1rem')

            category = ui.select(
                ['Bug', 'Feature Request', 'General Feedback', 'Job Opportunity'],
                value='General Feedback',
                label='Category'
            ).classes('w-full').style('font-size: 1.1rem')

            feedback = ui.textarea(
                label='Message',
                placeholder='Type your feedback here...',
            ).classes('w-full').props('outlined').style('font-size: 1.1rem')

            status = ui.label().classes('text-center').style('font-size: 1.1rem')

            def submit():
                if not feedback.value.strip():
                    status.set_text('Please enter a message.')
                    status.classes(add='text-red-600', remove='text-green-600')
                    return
                try:
                    send_feedback_email(
                        feedback.value,
                        email_input.value,
                        category.value,
                    )
                    email_input.value = ''
                    feedback.value = ''
                    status.set_text('Thanks! Your message was sent.')
                    status.classes(add='text-green-600', remove='text-red-600')
                except Exception as e:
                    status.set_text('Error sending feedback. Please try again later.')
                    status.classes(add='text-red-600', remove='text-green-600')

            ui.button('Send Feedback', on_click=submit).classes('w-full bg-blue-600 text-white').style('font-size: 1.1rem')

    footer()
    

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='SwimmingRank.org - Swimmer Progress and Analytics', reload='FLY_ALLOC_ID' not in os.environ)
    app.add_static_file(local_file='static/sitemap.txt', url_path='/sitemap')