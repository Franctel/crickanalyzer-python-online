from flask import Blueprint, render_template, request
from flask_login import login_required, current_user
import urllib
import numpy as np
import plotly.io as pio
from flask import Flask, render_template, request, jsonify, redirect, url_for
from tailwick.utils import (
    create_team_order_summary, generate_kpi_with_summary_tables, get_all_teams, get_all_tournaments, get_data_from_db, get_filtered_score_data, get_match_format_by_tournament, get_teams_by_tournament, get_matches_by_team, 
    get_days_innings_sessions_by_matches, get_players_by_match,
    get_ball_by_ball_details, generate_commentary,
    generate_line_length_report, generate_areawise_report, generate_shottype_report,
    generate_deliverytype_report, make_summary_table, render_summary_table, generate_player_vs_player_table, get_match_header, get_match_innings, get_last_12_deliveries, get_ball_by_ball_data, generate_wagon_wheel, create_partnership_chart, get_innings_deliveries, fetch_metric_videos, generate_multi_day_report, generate_session_radar_chart, generate_session_pitchmaps, generate_line_length_table_new, generate_partnership_table, get_connection, get_match_format_code_by_tournament, create_runs_per_over_chart, create_run_rate_chart, create_donut_charts, create_extra_runs_comparison_chart, create_comparison_bar_chart,  get_phase_definitions, generate_team_comparison_radar, create_player_contribution_donut, generate_batting_summary, create_bowling_dotball_donut, build_bowling_summary, generate_batting_vs_pace_spin, create_vs_pace_spin_chart, create_phase_partnership_chart, generate_delivery_type_distribution, generate_pitch_area_distribution, create_runs_conceded_chart_and_table, create_boundaries_conceded_chart_and_table, get_players_by_team, get_players_by_tournament, get_bowlers_by_team, get_bowlers_by_tournament, get_parent_video_path_from_db, no_data_figure, generate_scorecard_json, get_match_header, BATTER_DATA, generate_heatmap_matrix, generate_team_wagon_radar, compute_area_stats, create_team_order_summary, get_team_inning_distribution, create_team_vs_opponent_runs_per_over_chart, create_team_vs_opponent_run_rate_chart, get_powerplay_stats, get_phase_stats_bowling
)


import pandas as pd
import pyodbc

apps = Blueprint('apps',__name__,template_folder='templates',
    static_folder='static')
    

@apps.route('/apps/<string:template_name>')
def dynamic_template_apps_view(template_name):
    return render_template(f'apps/{template_name}.html')

@apps.route('/apps/calendar/<string:template_name>')
def dynamic_template_calendar_view(template_name):
    return render_template(f'apps/calendar/{template_name}.html')

@apps.route('/apps/ecommerce/<string:template_name>')
def dynamic_template_ecommerce_view(template_name):
    return render_template(f'apps/ecommerce/{template_name}.html')

@apps.route('/apps/hr/<string:template_name>')
def dynamic_template_hr_view(template_name):
    return render_template(f'apps/hr/{template_name}.html')    

@apps.route('/apps/social/<string:template_name>')
def dynamic_template_social_view(template_name):
    return render_template(f'apps/social/{template_name}.html')

@apps.route('/apps/invoice/<string:template_name>')
def dynamic_template_invoice_view(template_name):
    return render_template(f'apps/invoice/{template_name}.html')

@apps.route('/apps/users/<string:template_name>')
def dynamic_template_users_view(template_name):
    return render_template(f'apps/users/{template_name}.html')



# ISPL Reports route
@apps.route('/apps/ispl-reports', methods=['GET', 'POST'])
def ispl_reports():
    import pandas as pd
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    # ==========================================================
    # ✅ TOURNAMENT DROPDOWN  (value=id, label=name)
    # ==========================================================
    tournaments_raw = get_all_tournaments(association_id) or []
    tournaments = [
        {"value": int(t["id"]), "label": str(t["name"])}
        for t in tournaments_raw
        if isinstance(t, dict) and t.get("id") and t.get("name")
    ]

    # ==========================================================
    # ✅ Read selected values (IDs)  ✅ SINGLE MATCH SELECT
    # ==========================================================
    if request.method == "POST":
        selected_tournament = request.form.get("tournament")   # tournament_id
        selected_team = request.form.get("team")               # team_id
        selected_match = request.form.get("matches")           # ✅ single match_id
    else:
        selected_tournament = request.args.get("tournament")
        selected_team = request.args.get("team")
        selected_match = request.args.get("matches")           # ✅ single match_id

    # ✅ Keep as list so existing template logic doesn't break
    selected_matches = [selected_match] if selected_match else []

    # ==========================================================
    # ✅ Safe conversions
    # ==========================================================
    try:
        selected_tournament_id = int(selected_tournament) if selected_tournament else None
    except:
        selected_tournament_id = None

    try:
        selected_team_id = int(selected_team) if selected_team else None
    except:
        selected_team_id = None

    # ✅ Convert selected_match into list of int ids
    selected_match_ids = []
    if selected_match:
        try:
            selected_match_ids = [int(selected_match)]
        except:
            selected_match_ids = []

    # ==========================================================
    # ✅ TEAM DROPDOWN  (value=id, label=name)
    # ==========================================================
    teams = []
    if selected_tournament_id:
        try:
            conn = get_connection()

            q = """
                SELECT DISTINCT t.tmM_Id AS id, t.tmM_ShortName AS name
                FROM tblscoremaster s
                INNER JOIN tblteammaster t
                    ON t.tmM_Id IN (s.scrM_tmMIdBatting, s.scrM_tmMIdBowling)
                WHERE s.scrM_TrnMId = %s
                ORDER BY t.tmM_ShortName
            """

            df_teams = pd.read_sql(q, conn, params=(selected_tournament_id,))
            conn.close()

            teams = [
                {"value": int(r["id"]), "label": str(r["name"])}
                for _, r in df_teams.iterrows()
                if r.get("id") and r.get("name")
            ]

        except Exception as e:
            print("❌ ISPL team dropdown error:", e)
            teams = []

    # ==========================================================
    # ✅ MATCH DROPDOWN  (value=id, label=name)
    # ==========================================================
    matches = []
    if selected_tournament_id and selected_team_id:
        try:
            conn = get_connection()

            q = """
                SELECT DISTINCT s.scrM_MchMId AS id, s.scrM_MatchName AS name
                FROM tblscoremaster s
                WHERE s.scrM_TrnMId = %s
                  AND (s.scrM_tmMIdBatting = %s OR s.scrM_tmMIdBowling = %s)
                  AND s.scrM_MatchName IS NOT NULL
                  AND s.scrM_MatchName <> ''
                ORDER BY s.scrM_MatchName
            """

            dfm = pd.read_sql(
                q,
                conn,
                params=(selected_tournament_id, selected_team_id, selected_team_id)
            )
            conn.close()

            matches = [
                {"value": int(r["id"]), "label": str(r["name"])}
                for _, r in dfm.iterrows()
                if r.get("id") and r.get("name")
            ]

        except Exception as e:
            print("❌ ISPL matches dropdown error:", e)
            matches = []

    # ==========================================================
    # ✅ REPORTS DEFAULTS
    # ==========================================================
    powerplay_report = []
    powerplay_sql_debug = None

    tapeball_deliveries = []
    tapeball_summary = {'Runs': 0, 'Wkts': 0, 'Balls': 0}

    fifty_deliveries = []
    fifty_summary = {'Runs': 0, 'Wkts': 0, 'Balls': 0, 'FFTarget': None}
    fifty_error = None

    # ==========================================================
    # ✅ Generate report ONLY if match_id selected
    # ==========================================================
    if selected_match_ids and selected_team_id:
        match_id = selected_match_ids[0]  # ✅ single match

        try:
            from tailwick.utils import (
                get_powerplay_overs,
                get_powerplay_stats_ISPL,
                get_tapeball_deliveries,
                get_fiftyover_deliveries
            )

            # ✅ Convert match_id -> match_name
            match_name = None
            try:
                conn = get_connection()
                df_mn = pd.read_sql(
                    """
                    SELECT scrM_MatchName
                    FROM tblscoremaster
                    WHERE scrM_MchMId = %s
                    LIMIT 1
                    """,
                    conn,
                    params=(match_id,)
                )
                conn.close()

                if not df_mn.empty:
                    match_name = str(df_mn.iloc[0]["scrM_MatchName"])
            except Exception as e:
                print("❌ match_name resolve error:", e)

            if match_name:
                # ✅ Powerplay
                pp_overs, powerplay_sql_debug = get_powerplay_overs(match_name, selected_team_id)

                for pp in pp_overs:
                    stats = get_powerplay_stats_ISPL(match_name, selected_team_id, pp["From"], pp["To"])
                    powerplay_report.append({
                        "PowerplayNo": pp["PowerplayNo"],
                        "From": pp["From"],
                        "To": pp["To"],
                        **stats
                    })

                # ✅ Tape Ball
                try:
                    tapeball_deliveries, tapeball_summary = get_tapeball_deliveries(match_name, selected_team_id)
                except Exception as e:
                    print("❌ tapeball error:", e)

                # ✅ 50-50 Over
                try:
                    fifty_res = get_fiftyover_deliveries(match_name, selected_team_id)
                    if isinstance(fifty_res, tuple) and len(fifty_res) == 3:
                        fifty_deliveries, fifty_summary, fifty_error = fifty_res
                    else:
                        fifty_deliveries, fifty_summary = fifty_res
                        fifty_error = None
                except Exception as e:
                    fifty_deliveries = []
                    fifty_summary = {'Runs': 0, 'Wkts': 0, 'Balls': 0, 'FFTarget': None}
                    fifty_error = str(e)

        except Exception as e:
            print("❌ ISPL Report generation error:", e)

    return render_template(
        'ispl_reports.html',
        tournaments=tournaments,
        teams=teams,
        matches=matches,
        selected_tournament=selected_tournament_id,
        selected_team=selected_team_id,
        selected_matches=selected_matches,   # ✅ still list

        powerplay_report=powerplay_report,
        powerplay_sql_debug=powerplay_sql_debug,
        tapeball_deliveries=tapeball_deliveries,
        tapeball_summary=tapeball_summary,
        fifty_deliveries=fifty_deliveries,
        fifty_summary=fifty_summary,
        fifty_error=fifty_error
    )




@apps.route('/dashboards-analytics', methods=['GET', 'POST'])
@login_required
def dashboards_analytics():
    teams = get_all_teams()
    selected_orders = []
    team1_table = team2_table = team1_summary = team2_summary = ""
    team1_name = team2_name = ""

    if request.method == 'POST':
        team1 = request.form.get('team1')
        team2 = request.form.get('team2')
        selected_orders = request.form.getlist('choices-order')
        df = get_data_from_db(team1, team2)

        for inn in [1, 2]:
            team, summary_df = create_team_order_summary(df, inn)
            if summary_df is not None:
                filtered_df = summary_df[summary_df["Order"].isin(selected_orders + ["Team Total"])]
                table_html = render_summary_table(team, filtered_df)
                summary_html = make_summary_table(team, filtered_df, selected_orders)

                if team == team1:
                    team1_table = table_html
                    team1_summary = summary_html
                    team1_name = team
                elif team == team2:
                    team2_table = table_html
                    team2_summary = summary_html
                    team2_name = team

    return render_template(
        "dashboard/dashboards-analytics.html",
        teams=teams,
        selected_orders=selected_orders,
        team1_table=team1_table,
        team2_table=team2_table,
        team1_summary=team1_summary,
        team2_summary=team2_summary,
        team1_name=team1_name,
        team2_name=team2_name
    )


@apps.route('/apps/player-analysis-1', methods=['GET', 'POST'])
@login_required
def advanced_filters_1():
    # 🆕 Filter tournaments by logged-in user's association
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    tournaments = get_all_tournaments(association_id)

    selected_tournaments = request.form.getlist("tournaments[]") if request.method == "POST" else []
    selected_teams = request.form.getlist("team[]") if request.method == "POST" else []

    selected_tournament = selected_tournaments[0] if selected_tournaments else None
    selected_team = selected_teams[0] if selected_teams else None

    selected_matches = request.form.getlist("matches[]") if request.method == "POST" else []


    # Selected player (single) and format override (quick select)
    selected_player = request.form.get("player") if request.method == "POST" else None
    selected_format_override = request.form.get("format_override") if request.method == "POST" else None

    # Support multi-select tournaments/teams from the Player Analysis 1 UI
    selected_tournaments = request.form.getlist('tournaments[]') if request.method == 'POST' else []
    if selected_tournaments and not selected_tournament:
        selected_tournament = selected_tournaments[0]

    selected_teams = request.form.getlist('team[]') if request.method == 'POST' else []
    if selected_teams and not selected_team:
        selected_team = selected_teams[0]

    # 🆕 Handle 'All' selection
    # ✅ Handle 'All' selection (MUST return match IDs)
    try:
        if 'All' in selected_matches and selected_team:
            conn = get_connection()

            # ✅ tournament filter supports multi-select OR single tournament
            tournament_ids = selected_tournaments if selected_tournaments else ([selected_tournament] if selected_tournament else [])

            if tournament_ids:
                placeholders = ",".join(["%s"] * len(tournament_ids))

                q = f"""
                    SELECT DISTINCT mchM_Id
                    FROM tblmatchmaster
                    WHERE (mchM_tmMId1 = %s OR mchM_tmMId2 = %s)
                    AND mchM_TrnMId IN ({placeholders})
                    ORDER BY mchM_StartDateTime DESC
                """

                params = [int(selected_team), int(selected_team)] + [int(x) for x in tournament_ids]

            else:
                # fallback (if no tournament selected)
                q = """
                    SELECT DISTINCT mchM_Id
                    FROM tblmatchmaster
                    WHERE (mchM_tmMId1 = %s OR mchM_tmMId2 = %s)
                    ORDER BY mchM_StartDateTime DESC
                """
                params = [int(selected_team), int(selected_team)]

            df_all = pd.read_sql(q, conn, params=tuple(params))
            conn.close()

            if df_all is not None and not df_all.empty:
                selected_matches = df_all["mchM_Id"].astype(int).astype(str).tolist()

    except Exception as e:
        print("⚠️ Error applying All matches logic:", e)



    # 🧹 Sanitize multiday filter inputs
    def sanitize_int_field(value):
        try:
            if not value or "select" in str(value).lower():
                return None
            return int(value)
        except Exception:
            return None

    selected_day_raw = request.form.get("day") or request.args.get("day")
    selected_inning_raw = request.form.get("inning") or request.args.get("inning")
    selected_session_raw = request.form.get("session") or request.args.get("session")

    selected_day = sanitize_int_field(selected_day_raw)
    selected_inning = sanitize_int_field(selected_inning_raw)
    selected_session = sanitize_int_field(selected_session_raw)

    selected_phase = request.form.get("phase") if request.method == "POST" else None
    from_over = request.form.get("from_over") if request.method == "POST" else None
    to_over = request.form.get("to_over") if request.method == "POST" else None
    selected_type = request.form.get("type") if request.method == "POST" else "batter"
    selected_ball_phase = request.form.get("ball_phase") if request.method == "POST" else None
    selected_metric = request.form.get("metric") if request.method == "POST" else None


    match_format = None
    # 🆕 Default radar vars
    radar_stats, radar_labels, radar_breakdown = None, None, None

    # Prefer explicit format override from the form; fall back to tournament-derived format
    if selected_format_override:
        try:
            match_format = str(selected_format_override).lower()
        except Exception:
            match_format = None
    elif selected_tournament:
        try:
            format_result = get_match_format_by_tournament(selected_tournament)
            match_format = format_result.lower() if format_result else None
        except Exception as e:
            print("Failed to get match format:", e)

    # ✅ AUTO LOAD MATCH IDS if Player + Format selected but Matches not selected
    # ✅ AUTO LOAD MATCH IDS (Tournament + Player + Format) if Matches not selected
    try:
        if request.method == "POST" and selected_player and match_format and not selected_matches:
            conn = get_connection()

            q = """
                SELECT DISTINCT m.mchM_Id
                FROM tblmatchmaster m
                INNER JOIN tblscoremaster s ON s.scrM_MchMId = m.mchM_Id
                INNER JOIN tbltournaments t ON t.trnM_Id = m.mchM_TrnMId
                INNER JOIN tblz z ON z.z_Id = t.trnM_MatchFormat_z
                WHERE (
                    s.scrM_PlayMIdStriker = %s OR
                    s.scrM_PlayMIdBowler = %s OR
                    s.scrM_PlayMIdNonStriker = %s
                )
                AND LOWER(z.z_Name) LIKE %s
            """

            params = [
                int(selected_player), int(selected_player), int(selected_player),
                f"%{match_format.lower()}%"
            ]

            # ✅ Tournament filter (single tournament dropdown OR multiselect tournament)
            if selected_tournaments:
                q += " AND m.mchM_TrnMId IN (" + ",".join(["%s"] * len(selected_tournaments)) + ")"
                params.extend([int(x) for x in selected_tournaments])
            elif selected_tournament:
                q += " AND m.mchM_TrnMId = %s"
                params.append(int(selected_tournament))

            q += " ORDER BY m.mchM_StartDateTime DESC"

            df_auto_matches = pd.read_sql(q, conn, params=tuple(params))
            conn.close()

            if df_auto_matches is not None and not df_auto_matches.empty:
                selected_matches = df_auto_matches["mchM_Id"].astype(str).tolist()
                print(f"✅ AUTO MATCH IDS LOADED (Tournament filtered): {len(selected_matches)} matches")

    except Exception as e:
        print("⚠️ Error auto loading matches by player+format+tournament:", e)





    # Build `teams` as list of {value,label} dicts based on selected tournaments (supports multi-select)
    # ✅ Build `teams` ONLY for selected player (based on batting/bowling role)
    teams = []
    matches = []

    try:
        if selected_tournaments and selected_player:
            conn = get_connection()
            placeholders = ",".join(["%s"] * len(selected_tournaments))

            q = f"""
                SELECT DISTINCT team_id, team_name
                FROM (
                    -- ✅ If player is striker/non-striker → batting team is player's team
                    SELECT
                        s.scrM_tmMIdBatting AS team_id,
                        s.scrM_tmMIdBattingName AS team_name
                    FROM tblscoremaster s
                    INNER JOIN tblmatchmaster m ON s.scrM_MchMId = m.mchM_Id
                    WHERE m.mchM_TrnMId IN ({placeholders})
                    AND (s.scrM_PlayMIdStriker = %s OR s.scrM_PlayMIdNonStriker = %s)

                    UNION

                    -- ✅ If player is bowler → bowling team is player's team
                    SELECT
                        s.scrM_tmMIdBowling AS team_id,
                        s.scrM_tmMIdBowlingName AS team_name
                    FROM tblscoremaster s
                    INNER JOIN tblmatchmaster m ON s.scrM_MchMId = m.mchM_Id
                    WHERE m.mchM_TrnMId IN ({placeholders})
                    AND (s.scrM_PlayMIdBowler = %s)
                ) x
                WHERE team_id IS NOT NULL AND team_name IS NOT NULL AND team_name <> ''
                ORDER BY team_name
            """

            params = (
                [int(x) for x in selected_tournaments]
                + [int(selected_player), int(selected_player)]
                + [int(x) for x in selected_tournaments]
                + [int(selected_player)]
            )

            df_teams = pd.read_sql(q, conn, params=tuple(params))
            conn.close()

            if df_teams is not None and not df_teams.empty:
                teams = [
                    {"value": str(int(r["team_id"])), "label": str(r["team_name"])}
                    for _, r in df_teams.iterrows()
                ]

        # ✅ fallback if no player selected → show tournament teams (your old logic can remain)
        elif selected_tournaments:
            conn = get_connection()
            placeholders = ",".join(["%s"] * len(selected_tournaments))

            q = f"""
                SELECT DISTINCT
                    COALESCE(s.scrM_tmMIdBatting, s.scrM_tmMIdBowling) AS team_id,
                    COALESCE(s.scrM_tmMIdBattingName, s.scrM_tmMIdBowlingName) AS team_name
                FROM tblscoremaster s
                INNER JOIN tblmatchmaster m ON s.scrM_MchMId = m.mchM_Id
                WHERE m.mchM_TrnMId IN ({placeholders})
                AND (s.scrM_tmMIdBatting IS NOT NULL OR s.scrM_tmMIdBowling IS NOT NULL)
                ORDER BY team_name
            """

            params = [int(x) for x in selected_tournaments]
            df_teams = pd.read_sql(q, conn, params=tuple(params))
            conn.close()

            if df_teams is not None and not df_teams.empty:
                teams = [
                    {"value": str(int(r["team_id"])), "label": str(r["team_name"])}
                    for _, r in df_teams.iterrows()
                ]

    except Exception as e:
        print("❌ Error building player-based teams:", e)
        teams = []


    # Build matches list: return list of dicts {value: match_id, label: match_name}
    try:
        matches = []
        # prefer multi-select team[] values when present
        sel_team_list = selected_teams if selected_teams else ([selected_team] if selected_team else [])

        if sel_team_list:
            num_ids = [int(x) for x in sel_team_list if str(x).isdigit()]
            name_vals = [x for x in sel_team_list if not str(x).isdigit()]
            match_candidates = []

            # numeric team ids -> query matchmaster for ids and names
            if num_ids:
                conn = get_connection()
                team_placeholders = ",".join(["%s"] * len(num_ids))
                if selected_tournaments:
                    trn_placeholders = ",".join(["%s"] * len(selected_tournaments))
                    q = f"""
                        SELECT DISTINCT m.mchM_Id, m.mchM_MatchName
                        FROM tblmatchmaster m
                        WHERE (m.mchM_tmMId1 IN ({team_placeholders}) OR m.mchM_tmMId2 IN ({team_placeholders}))
                          AND m.mchM_TrnMId IN ({trn_placeholders})
                        ORDER BY m.mchM_StartDateTime DESC
                    """
                    params = tuple(num_ids + num_ids + [int(x) for x in selected_tournaments])
                else:
                    q = f"""
                        SELECT DISTINCT m.mchM_Id, m.mchM_MatchName
                        FROM tblmatchmaster m
                        WHERE (m.mchM_tmMId1 IN ({team_placeholders}) OR m.mchM_tmMId2 IN ({team_placeholders}))
                        ORDER BY m.mchM_StartDateTime DESC
                    """
                    params = tuple(num_ids + num_ids)

                df_matches = pd.read_sql(q, conn, params=params)
                conn.close()
                if df_matches is not None and not df_matches.empty:
                    for _, r in df_matches.iterrows():
                        mid = r.get('mchM_Id')
                        mname = r.get('mchM_MatchName')
                        if pd.notna(mid) and pd.notna(mname):
                            match_candidates.append({"value": str(int(mid)), "label": str(mname)})

            # name-based teams: use helper to fetch match names (fallback)
            for nm in name_vals:
                try:
                    ms = get_matches_by_team(nm, selected_tournament)
                    if ms:
                        # helper returns match names; use name as value as fallback
                        for mn in ms:
                            match_candidates.append({"value": str(mn), "label": str(mn)})
                except Exception:
                    continue

            # dedupe preserving order
            seen = set()
            matches = [m for m in match_candidates if not (m['value'] in seen or seen.add(m['value']))]

        else:
            # legacy single selected_team behavior
            if selected_team:
                if str(selected_team).isdigit():
                    conn = get_connection()
                    if selected_tournaments:
                        placeholders = ",".join(["%s"] * len(selected_tournaments))
                        q = f"""
                            SELECT DISTINCT m.mchM_Id, m.mchM_MatchName
                            FROM tblmatchmaster m
                            WHERE (m.mchM_tmMId1 = %s OR m.mchM_tmMId2 = %s)
                              AND m.mchM_TrnMId IN ({placeholders})
                            ORDER BY m.mchM_StartDateTime DESC
                        """
                        params = tuple([int(selected_team), int(selected_team)] + [int(x) for x in selected_tournaments])
                    else:
                        q = """
                            SELECT DISTINCT m.mchM_Id, m.mchM_MatchName
                            FROM tblmatchmaster m
                            WHERE (m.mchM_tmMId1 = %s OR m.mchM_tmMId2 = %s)
                            ORDER BY m.mchM_StartDateTime DESC
                        """
                        params = (int(selected_team), int(selected_team))
                    df_matches = pd.read_sql(q, conn, params=params)
                    conn.close()
                    if df_matches is not None and not df_matches.empty:
                        matches = [{"value": str(int(r['mchM_Id'])), "label": r['mchM_MatchName']} for _, r in df_matches.iterrows()]
                else:
                    # fallback: helper returns names
                    ms = get_matches_by_team(selected_team, selected_tournament)
                    if ms:
                        matches = [{"value": str(mn), "label": str(mn)} for mn in ms]
    except Exception as e:
        print('Error building matches list by team id:', e)

    # If matches were returned as id/label dicts, map any selected match ids
    # (from the POST) back to match names so downstream code expecting
    # match names continues to work.
    # try:
    #     if matches and isinstance(matches, list) and isinstance(matches[0], dict):
    #         value_to_label = {m['value']: m['label'] for m in matches}
    #         # request.form.getlist was stored in selected_matches earlier
    #         mapped = []
    #         for sm in selected_matches:
    #             if sm in value_to_label:
    #                 mapped.append(value_to_label[sm])
    #             else:
    #                 # keep existing value if not found (could be 'All' or legacy name)
    #                 mapped.append(sm)
    #         selected_matches = mapped
    # except Exception as e:
    #     print('Error mapping selected match ids to names:', e)


    days, innings, sessions = [], [], []
    # Fetch all players from DB for Player Analysis 1 (value = player id)
    players = []
    try:
        conn = get_connection()
        # try richer query first (may fail if columns differ)
        try:
            df_players = pd.read_sql(
                "SELECT playM_Id, playM_PlayerName, playM_BattingStyle, playM_BowlingStyle FROM tblplayers",
                conn,
            )
        except Exception:
            # fallback to minimal columns if schema differs
            df_players = pd.read_sql(
                "SELECT playM_Id, playM_PlayerName FROM tblplayers",
                conn,
            )
        conn.close()

        if not df_players.empty:
            for _, r in df_players.iterrows():
                pid = r.get('playM_Id')
                name = (r.get('playM_PlayerName') or '')
                # try both possible style column names if present
                bat = r.get('playM_BattingStyle') if 'playM_BattingStyle' in r.index else r.get('playM_BattingStyle_z') if 'playM_BattingStyle_z' in r.index else ''
                bowl = r.get('playM_BowlingStyle') if 'playM_BowlingStyle' in r.index else r.get('playM_BowlingStyle_z') if 'playM_BowlingStyle_z' in r.index else ''
                skills = []
                if pd.notna(bat) and str(bat).strip():
                    skills.append(str(bat).strip())
                if pd.notna(bowl) and str(bowl).strip():
                    skills.append(str(bowl).strip())
                label = name
                if skills:
                    label = f"{name} ({'/'.join(skills)})"
                try:
                    pid_str = str(int(pid)) if pid is not None else ''
                except Exception:
                    pid_str = str(pid) if pid is not None else ''
                players.append({'id': pid_str, 'label': label})

        print(f"DEBUG: loaded {len(players)} players for dropdown")
        if len(players) > 0:
            print("DEBUG: sample players:", players[:10])
        # --- If a player is pre-selected, move them to front of the players dropdown ---
        try:
            if selected_player and players:
                sid = str(selected_player)
                sel_idx = None
                for i, p in enumerate(players):
                    if str(p.get('id')) == sid:
                        sel_idx = i
                        break
                    # also allow matching by name token
                    if p.get('label') and p.get('label').split(' (')[0] == sid:
                        sel_idx = i
                        break
                if sel_idx is not None and sel_idx != 0:
                    sel = players.pop(sel_idx)
                    players.insert(0, sel)
        except Exception as e:
            print('Error prioritizing selected player in players list:', e)
    except Exception as e:
        print('Error loading players for dropdown:', e)

    # ------------------ New: Filter tournaments by selected player + format ------------------
    try:
        # If a player is selected, find tournaments where this player appears and optionally match format
        if selected_player:
            conn = get_connection()
            # First, if a format_override is provided, try to resolve it to z.z_Id
            format_z_id = None
            if selected_format_override:
                try:
                    df_z = pd.read_sql(
                        "SELECT z_Id FROM tblz WHERE LOWER(z_Name) LIKE %s LIMIT 1",
                        conn,
                        params=(f"%{selected_format_override.lower()}%",),
                    )
                    if not df_z.empty:
                        format_z_id = int(df_z.iloc[0]['z_Id'])
                except Exception:
                    format_z_id = None

            # Query tournaments where this player id appears in scoremaster for matches linked to tournaments
            # Use PlayMId columns which store numeric player ids where available
            q = """
                SELECT DISTINCT t.trnM_Id, t.trnM_TournamentName, t.trnM_MatchFormat_z
                FROM tbltournaments t
                INNER JOIN tblmatchmaster m ON m.mchM_TrnMId = t.trnM_Id
                INNER JOIN tblscoremaster s ON s.scrM_MchMId = m.mchM_Id
                WHERE (
                    s.scrM_PlayMIdStriker = %s OR s.scrM_PlayMIdBowler = %s OR s.scrM_PlayMIdNonStriker = %s
                )
            """

            params = (selected_player, selected_player, selected_player)

            # If format_z_id found, restrict to that format id
            if format_z_id:
                q = q.strip() + " AND t.trnM_MatchFormat_z = %s"
                params = tuple(list(params) + [format_z_id])

            df_t = pd.read_sql(q, conn, params=params)
            conn.close()

            if df_t is not None and not df_t.empty:
                # Build tournament options from this filtered set
                tournaments = [
                    {"value": int(r['trnM_Id']), "label": r['trnM_TournamentName']}
                    for _, r in df_t.iterrows()
                ]

                # If format not explicitly chosen by user, try to infer and set it in the form
                if not selected_format_override:
                    try:
                        # prefer T20 -> ODI -> T10 -> Test ordering if present
                        df_formats = df_t['trnM_MatchFormat_z'].dropna().astype(int).unique().tolist()
                        if df_formats:
                            # fetch z names for these ids
                            conn2 = get_connection()
                            fmt_df = pd.read_sql(
                                f"SELECT z_Id, z_Name FROM tblz WHERE z_Id IN ({','.join(['%s']*len(df_formats))})",
                                conn2, params=tuple(df_formats)
                            )
                            conn2.close()
                            # normalize names
                            fmt_map = {int(r['z_Id']): (r['z_Name'] or '').lower() for _, r in fmt_df.iterrows()}
                            # ordering preference
                            pref = ['t20', 'odi', 't10', 'test']
                            chosen = None
                            for p in pref:
                                for fid, name in fmt_map.items():
                                    if p in name:
                                        chosen = name
                                        break
                                if chosen:
                                    break
                            if not chosen:
                                # pick first available
                                chosen = list(fmt_map.values())[0]
                            # set in request.form so template shows it as selected
                            request.form = request.form.copy()
                            request.form = request.form
                            # set format_override to chosen keyword (use simple tokens: t20/odi/t10/test)
                            token = 't20' if '20' in chosen else ('odi' if 'odi' in chosen else ('t10' if '10' in chosen else ('test' if 'test' in chosen else chosen)))
                            request.form = request.form.copy()
                            request.form['format_override'] = token
                    except Exception as e:
                        print('Error inferring format for player tournaments:', e)

                # --- Auto-load tournaments/matches for selected player+format ---
                try:
                    # If no explicit multi-select tournaments provided, expose all tournaments
                    if not selected_tournaments:
                        selected_tournaments = [int(r['trnM_Id']) for _, r in df_t.iterrows() if pd.notna(r['trnM_Id'])]

                    # If user hasn't selected matches explicitly, load all matches for this player
                    # restricted to the tournaments discovered above (and format if applied).
                    if not selected_matches:
                        conn2 = get_connection()
                        params2 = [selected_player, selected_player, selected_player]
                        q2 = """
                            SELECT DISTINCT m.mchM_MatchName
                            FROM tblmatchmaster m
                            INNER JOIN tblscoremaster s ON s.scrM_MchMId = m.mchM_Id
                            WHERE (
                                s.scrM_PlayMIdStriker = %s OR s.scrM_PlayMIdBowler = %s OR s.scrM_PlayMIdNonStriker = %s
                            )
                        """
                        if selected_tournaments:
                            q2 = q2.strip() + " AND m.mchM_TrnMId IN (" + ",".join(["%s"]*len(selected_tournaments)) + ")"
                            params2.extend(selected_tournaments)

                        q2 = q2 + " ORDER BY m.mchM_StartDateTime DESC"
                        df_matches_player = pd.read_sql(q2, conn2, params=tuple(params2))
                        conn2.close()
                        if df_matches_player is not None and not df_matches_player.empty:
                            # populate selected_matches with match names so reports will load
                            selected_matches = [str(r['mchM_MatchName']) for _, r in df_matches_player.iterrows()]
                except Exception as e:
                    print('Error auto-loading matches for player:', e)

    except Exception as e:
        print('Error filtering tournaments by player/format:', e)
    batters, bowlers = [], []
    kpi_tables = {}
    ball_by_ball_details = []
    line_length_report = None
    areawise_report = None
    shottype_report = None
    deliverytype_report = None

    # Helper: determine total overs from match_format or data fallback
    def infer_total_overs(fmt, df_for_infer=None):
        try:
            fmt = (fmt or "").lower()
            if 't10' in fmt or ('10' in fmt and 't20' not in fmt and '50' not in fmt and 'odi' not in fmt):
                return 10
            if 't20' in fmt or 'twenty' in fmt or (('20' in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                return 20
            if 'odi' in fmt or 'one day' in fmt or '50' in fmt:
                return 50
            # fallback: infer from data passed (ball_by_ball or df)
            if df_for_infer is not None and not df_for_infer.empty:
                # compute max over number present in data
                if 'scrM_OverNo' in df_for_infer.columns:
                    try:
                        max_over = int(df_for_infer['scrM_OverNo'].max())
                        if max_over in (10, 20, 50):
                            return max_over
                    except Exception:
                        pass
            # default fallback: assume 20
            return 20
        except Exception:
            return 20

    if selected_matches:
        print("Entered 'if selected_matches' block.")
        try:
            # Filters
            days, innings, sessions = get_days_innings_sessions_by_matches(selected_matches)

            # Player Lists
            # batters, bowlers = get_players_by_match(
            #     selected_matches,
            #     day=selected_day,
            #     inning=selected_inning,
            #     session=selected_session
            # )

            # ✅ Player-based opponent dropdown lists (CORRECT)
            batters, bowlers = [], []

            try:
                if selected_matches and selected_player:

                    pid = int(selected_player)

                    conn = get_connection()
                    placeholders = ",".join(["%s"] * len(selected_matches))

                    # ✅ BATTER MODE → show ONLY bowlers faced by selected batter
                    # ✅ BATTER MODE → show ONLY bowlers faced by selected batter
                    if selected_type == "batter":
                        q = f"""
                            SELECT DISTINCT
                                s.scrM_PlayMIdBowler AS id,
                                s.scrM_PlayMIdBowlerName AS name,
                                MAX(NULLIF(TRIM(s.scrM_BowlerSkill), '')) AS skill
                            FROM tblscoremaster s
                            WHERE s.scrM_MchMId IN ({placeholders})
                            AND s.scrM_IsValidBall = 1
                            AND s.scrM_PlayMIdStriker = %s
                            GROUP BY s.scrM_PlayMIdBowler, s.scrM_PlayMIdBowlerName
                            ORDER BY name
                        """

                        params = tuple([int(x) for x in selected_matches] + [pid])
                        df_bowlers = pd.read_sql(q, conn, params=params)

                        bowlers = []
                        if df_bowlers is not None and not df_bowlers.empty:
                            df_bowlers["name"] = df_bowlers["name"].fillna("").astype(str).str.strip()
                            df_bowlers["skill"] = df_bowlers["skill"].fillna("").astype(str).str.strip()

                            for _, r in df_bowlers.iterrows():
                                bowler_name = str(r["name"]).strip()
                                bowler_skill = str(r["skill"]).strip()

                                # ✅ IMPORTANT: if skill is like "(RAMF)" extract RAMF
                                bowler_skill = bowler_skill.replace("(", "").replace(")", "").strip()

                                if not bowler_name:
                                    continue

                                label = f"{bowler_name} ({bowler_skill})" if bowler_skill else bowler_name

                                bowlers.append({
                                    "id": str(int(r["id"])) if str(r["id"]).isdigit() else str(r["id"]),
                                    "label": label
                                })

                        batters = []  # not required in batter mode


                    # ✅ BOWLER MODE → show ONLY batters who faced selected bowler
                    # ✅ BOWLER MODE → show ONLY batters who faced selected bowler
                    elif selected_type == "bowler":
                        q = f"""
                            SELECT DISTINCT
                                s.scrM_PlayMIdStriker AS id,
                                s.scrM_PlayMIdStrikerName AS name,
                                MAX(NULLIF(TRIM(s.scrM_StrikerBatterSkill), '')) AS skill
                            FROM tblscoremaster s
                            WHERE s.scrM_MchMId IN ({placeholders})
                            AND s.scrM_IsValidBall = 1
                            AND s.scrM_PlayMIdBowler = %s
                            GROUP BY s.scrM_PlayMIdStriker, s.scrM_PlayMIdStrikerName
                            ORDER BY name
                        """

                        params = tuple([int(x) for x in selected_matches] + [pid])
                        df_batters = pd.read_sql(q, conn, params=params)

                        batters = []
                        if df_batters is not None and not df_batters.empty:
                            df_batters["name"] = df_batters["name"].fillna("").astype(str).str.strip()
                            df_batters["skill"] = df_batters["skill"].fillna("").astype(str).str.strip()

                            for _, r in df_batters.iterrows():
                                batter_name = str(r["name"]).strip()
                                batter_skill = str(r["skill"]).strip()

                                batter_skill = batter_skill.replace("(", "").replace(")", "").strip()

                                if not batter_name:
                                    continue

                                label = f"{batter_name} ({batter_skill})" if batter_skill else batter_name

                                batters.append({
                                    "id": str(int(r["id"])) if str(r["id"]).isdigit() else str(r["id"]),
                                    "label": label
                                })

                        bowlers = []  # not required in bowler mode


                    conn.close()

            except Exception as e:
                print("⚠️ Error loading player-based opponent dropdowns:", e)


            # ✅ FIX: Ensure batters/bowlers always become list of names (string)
            # ✅ FIX: Ensure batters/bowlers always become list of names (string)
            # try:
            #     if batters and isinstance(batters[0], dict):
            #         batters = [str(x.get("name", "")).strip() for x in batters if x.get("name")]
            #     if bowlers and isinstance(bowlers[0], dict):
            #         bowlers = [str(x.get("name", "")).strip() for x in bowlers if x.get("name")]
            # except Exception as e:
            #     print("⚠️ Error converting batters/bowlers dict->string:", e)



            # ✅ Convert batters/bowlers to dict format required by HTML
            # batters = [{"id": str(b.split(" (")[0]), "name": b} for b in batters] if batters else []
            # bowlers = [{"id": str(b.split(" (")[0]), "name": b} for b in bowlers] if bowlers else []


            # ✅ Enhance dropdown labels: show hand type for batters and bowling skill for bowlers (for MySQL schema)
            try:
                conn = get_connection()
                placeholders = ",".join(["%s"] * len(selected_matches))

                query = f"""
                    SELECT
                        s.scrM_PlayMIdStrikerName AS Batter,
                        MAX(NULLIF(TRIM(s.scrM_StrikerBatterSkill), '')) AS BatterSkill,
                        s.scrM_PlayMIdBowlerName AS Bowler,
                        MAX(NULLIF(TRIM(s.scrM_BowlerSkill), '')) AS BowlerSkill
                    FROM tblscoremaster s
                    WHERE s.scrM_MchMId IN ({placeholders})
                    AND s.scrM_IsValidBall = 1
                    GROUP BY s.scrM_PlayMIdStrikerName, s.scrM_PlayMIdBowlerName
                """

                player_df = pd.read_sql(query, conn, params=tuple(selected_matches))
                conn.close()

                if not player_df.empty:
                    # Normalize skills
                    for col in ['BatterSkill', 'BowlerSkill']:
                        player_df[col] = (
                            player_df[col]
                            .fillna('')
                            .astype(str)
                            .str.strip()
                            .str.upper()
                        )
                        # Extract only short codes (e.g., RHB)
                        player_df[col] = player_df[col].str.extract(r'\(([A-Z]+)\)')[0].fillna(player_df[col])

                    # Build display labels
                    player_df['display_batter'] = player_df.apply(
                        lambda x: f"{x['Batter']} ({x['BatterSkill']})" if x['BatterSkill'] else x['Batter'],
                        axis=1
                    )
                    player_df['display_bowler'] = player_df.apply(
                        lambda x: f"{x['Bowler']} ({x['BowlerSkill']})" if x['BowlerSkill'] else x['Bowler'],
                        axis=1
                    )

                    batter_skill_map = dict(zip(player_df['Batter'], player_df['display_batter']))
                    bowler_skill_map = dict(zip(player_df['Bowler'], player_df['display_bowler']))

                    for b in bowlers:
                        base_name = b["label"].split(" (")[0]
                        b["label"] = bowler_skill_map.get(base_name, b["label"])

                    for b in batters:
                        base_name = b["label"].split(" (")[0]
                        b["label"] = batter_skill_map.get(base_name, b["label"])


                    # --- Prioritize selected player in batters/bowlers lists ---
                    # --- Prioritize selected player in batters/bowlers lists ---
                    try:
                        if selected_player:
                            pname = None
                            sid = str(selected_player)

                            for p in players:
                                if str(p.get("id")) == sid:
                                    pname = p.get("label", "").split(" (")[0]
                                    break

                            if pname is None:
                                pname = sid

                            def safe_name(x):
                                if isinstance(x, dict):
                                    return str(x.get("name", "")).strip()
                                return str(x).strip()

                            # Move batter to top
                            for i, v in enumerate(batters):
                                if safe_name(v).split(" (")[0] == pname:
                                    if i != 0:
                                        batters.insert(0, batters.pop(i))
                                    break

                            # Move bowler to top
                            for i, v in enumerate(bowlers):
                                if safe_name(v).split(" (")[0] == pname:
                                    if i != 0:
                                        bowlers.insert(0, bowlers.pop(i))
                                    break

                    except Exception as e:
                        print("⚠️ Error prioritizing selected player:", e)


            except Exception as e:
                print("⚠️ Error enriching dropdowns with skill info:", e)

            # 🆕 Handle “All” filters for Batters and Bowlers
            # 🆕 Handle “All” filters for Batters and Bowlers
            try:
                selected_batters = request.form.getlist("batters[]")
                selected_bowlers = request.form.getlist("bowlers[]")

                # preserve UI selection
                display_selected_batters = selected_batters.copy()
                display_selected_bowlers = selected_bowlers.copy()

                # ✅ helper to safely extract batter/bowler values
                def get_player_id(x):
                    if isinstance(x, dict):
                        return str(x.get("id", "")).strip()
                    return str(x).strip()

                def get_player_name(x):
                    if isinstance(x, dict):
                        return str(x.get("label", "")).strip()
                    return str(x).strip()

                # ---------------------------------------------------
                # ✅ BATTER FILTERS
                # batters can be list[str] OR list[dict]
                # ---------------------------------------------------
                internal_batters = []

                if any(str(opt).startswith("All") for opt in selected_batters):

                    if "All" in selected_batters:
                        internal_batters = [get_player_id(b) for b in batters]
                        display_selected_batters = ["All"]

                    elif "All (RHB)" in selected_batters:
                        internal_batters = [
                            get_player_id(b) for b in batters
                            if "(RHB)" in get_player_name(b).upper()
                        ]
                        display_selected_batters = ["All (RHB)"]

                    elif "All (LHB)" in selected_batters:
                        internal_batters = [
                            get_player_id(b) for b in batters
                            if "(LHB)" in get_player_name(b).upper()
                        ]
                        display_selected_batters = ["All (LHB)"]

                else:
                    internal_batters = [str(x) for x in selected_batters]

                if not internal_batters:
                    internal_batters = [get_player_id(b) for b in batters]

                # ---------------------------------------------------
                # ✅ BOWLER FILTERS
                # bowlers can be list[str] OR list[dict]
                # ---------------------------------------------------
                internal_bowlers = []

                if any(str(opt).startswith("All") for opt in selected_bowlers):

                    if "All" in selected_bowlers:
                        internal_bowlers = [get_player_id(b) for b in bowlers]
                        display_selected_bowlers = ["All"]

                    else:
                        selected_types = [s for s in selected_bowlers if str(s).startswith("All (")]
                        skills = [s.split("(")[1].replace(")", "").strip().upper() for s in selected_types]

                        internal_bowlers = [
                            get_player_id(b) for b in bowlers
                            if any(skill in get_player_name(b).upper() for skill in skills)
                        ]

                        display_selected_bowlers = [f"All ({'/'.join(skills)})"]

                else:
                    internal_bowlers = [str(x) for x in selected_bowlers]

                if not internal_bowlers:
                    internal_bowlers = [get_player_id(b) for b in bowlers]

                # ✅ Apply internally
                request.form = request.form.copy()
                request.form.setlist("batters[]", internal_batters)
                request.form.setlist("bowlers[]", internal_bowlers)

            except Exception as e:
                print("⚠️ Error applying 'All' filter logic:", e)




            
            df = None
            try:
                conn = get_connection()
                
                df = get_filtered_score_data(
                    conn,
                    selected_matches,
                    day=selected_day,
                    inning=selected_inning,
                    session=selected_session,
                    phase=selected_phase,
                    from_over=from_over,
                    to_over=to_over,
                    type=selected_type,
                    batters=request.form.getlist("batters[]"),
                    bowlers=request.form.getlist("bowlers[]")
                )
                conn.close()
                print(f"Data rows {len(df)} ")
                # --- FILTER df BY SELECTED TEAM (minimal change) ---
                if selected_team and df is not None and not df.empty:
                    try:
                        st = selected_team
                        # If numeric id provided, prefer id columns
                        if str(st).isdigit():
                            tid = int(st)
                            if selected_type == "batter":
                                if 'scrM_tmMIdBatting' in df.columns:
                                    df = df[df['scrM_tmMIdBatting'] == tid]
                                elif 'scrM_tmMIdBattingName' in df.columns:
                                    df = df[df['scrM_tmMIdBattingName'] == str(tid)]
                            else:
                                if 'scrM_tmMIdBowling' in df.columns:
                                    df = df[df['scrM_tmMIdBowling'] == tid]
                                elif 'scrM_tmMIdBowlingName' in df.columns:
                                    df = df[df['scrM_tmMIdBowlingName'] == str(tid)]
                        else:
                            if selected_type == "batter":
                                if 'scrM_tmMIdBattingName' in df.columns:
                                    df = df[df['scrM_tmMIdBattingName'] == st]
                                else:
                                    df = df[df['scrM_tmMIdBatting'] == st]
                            else:
                                if 'scrM_tmMIdBowlingName' in df.columns:
                                    df = df[df['scrM_tmMIdBowlingName'] == st]
                                else:
                                    df = df[df['scrM_tmMIdBowling'] == st]
                        print(f"Filtered df rows for team {selected_team}: {len(df)}")
                    except Exception as e:
                        print("Error filtering df by selected_team:", e)

                # --- FILTER df BY SELECTED PLAYER (ensure report shows only selected player) ---
                # --- ✅ FILTER df BY SELECTED PLAYER (ONLY selected player data) ---
                if selected_player and df is not None and not df.empty:
                    try:
                        pid_val = int(selected_player) if str(selected_player).isdigit() else selected_player

                        # Get selected player name from dropdown list (fallback support)
                        pname = None
                        for p in players:
                            if str(p.get("id")) == str(selected_player):
                                pname = p.get("label", "").split(" (")[0]
                                break

                        # ✅ STRICT filtering by selected_type
                        if selected_type == "batter":
                            # ONLY striker balls of selected batter
                            if "scrM_PlayMIdStriker" in df.columns and str(pid_val).isdigit():
                                df = df[df["scrM_PlayMIdStriker"] == int(pid_val)]
                            elif "scrM_PlayMIdStrikerName" in df.columns and pname:
                                df = df[df["scrM_PlayMIdStrikerName"] == pname]

                        elif selected_type == "bowler":
                            # ONLY bowler balls of selected bowler
                            if "scrM_PlayMIdBowler" in df.columns and str(pid_val).isdigit():
                                df = df[df["scrM_PlayMIdBowler"] == int(pid_val)]
                            elif "scrM_PlayMIdBowlerName" in df.columns and pname:
                                df = df[df["scrM_PlayMIdBowlerName"] == pname]

                        print(f"✅ Filtered df ONLY for selected player={selected_player}, rows={len(df)}")

                    except Exception as e:
                        print("❌ Error filtering df by selected_player:", e)

                # --- ✅ Apply PHASE filter to df (for Player vs Player consistency) ---
                if df is not None and not df.empty:
                    try:
                        # Normalize
                        sel_phase = str(selected_phase).strip().lower() if selected_phase else ""
                        sel_ball_phase = str(selected_ball_phase).strip().lower() if selected_ball_phase else ""
                        fmt = str(match_format).lower() if match_format else ""
                        total_overs = infer_total_overs(fmt, df_for_infer=df)
                        total_balls = total_overs * 6

                        # Ensure OverNo and DelNo exist and create ball_index
                        if 'scrM_OverNo' in df.columns and 'scrM_DelNo' in df.columns:
                            # safe convert
                            try:
                                df['scrM_OverNo'] = df['scrM_OverNo'].astype(int)
                                df['scrM_DelNo'] = df['scrM_DelNo'].astype(int)
                            except Exception:
                                df['scrM_OverNo'] = pd.to_numeric(df['scrM_OverNo'], errors='coerce').fillna(0).astype(int)
                                df['scrM_DelNo'] = pd.to_numeric(df['scrM_DelNo'], errors='coerce').fillna(0).astype(int)

                            df['ball_index'] = (df['scrM_OverNo'] - 1) * 6 + df['scrM_DelNo']
                        else:
                            df['ball_index'] = None

                        # Over-based phase filter (Powerplay/Middle/Death) — if selected_phase present
                        if sel_phase:
                            try:
                                pp_end, middle_end = 6, 15  # default for T20
                                if ('t10' in fmt) or (('10' in fmt) and ('t20' not in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                                    pp_end, middle_end = 3, 7
                                elif ('t20' in fmt) or ('twenty' in fmt) or (('20' in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                                    pp_end, middle_end = 6, 15
                                elif ('odi' in fmt) or ('one day' in fmt) or ('50' in fmt):
                                    pp_end, middle_end = 10, 40
                                else:
                                    # infer from max over in df
                                    try:
                                        max_over = int(df['scrM_OverNo'].max())
                                        if max_over == 50:
                                            pp_end, middle_end = 10, 40
                                        elif max_over == 20:
                                            pp_end, middle_end = 6, 15
                                        elif max_over == 10:
                                            pp_end, middle_end = 3, 7
                                    except Exception:
                                        pass

                                if sel_phase not in ('all', ''):
                                    if 'power' in sel_phase:
                                        df = df[df['scrM_OverNo'] <= pp_end]
                                    elif 'middle' in sel_phase:
                                        df = df[(df['scrM_OverNo'] > pp_end) & (df['scrM_OverNo'] <= middle_end)]
                                    elif 'death' in sel_phase or 'slog' in sel_phase:
                                        df = df[df['scrM_OverNo'] > middle_end]
                            except Exception as e:
                                print("Error filtering df for phase:", e)

                        # Ball-phase filtering (First 10 / Middle Balls / Last 10) applied on ball_index (per innings)
                        # ✅ Apply per-batter or per-bowler ball-phase filtering (consistent with offline logic)
                        if sel_ball_phase and 'ball_index' in df.columns:
                            try:
                                key_col = 'scrM_PlayMIdStrikerName' if selected_type == 'batter' else 'scrM_PlayMIdBowlerName'
                                phase_dfs = []

                                for name, sub in df.groupby(key_col, group_keys=False):
                                    sub = sub.sort_values(['scrM_OverNo', 'scrM_DelNo']).reset_index(drop=True)
                                    total_balls = len(sub)

                                    # If player bowled/faced ≤10 balls, include all
                                    if total_balls <= 10:
                                        phase_dfs.append(sub)
                                        continue

                                    sel = sel_ball_phase.lower().strip()

                                    # 🟢 First 10 balls
                                    if 'first' in sel:
                                        phase_dfs.append(sub.head(10))

                                    # 🟠 Middle Balls (exclude first 10 and last 10)
                                    elif 'middle' in sel:
                                        if total_balls > 20:
                                            phase_dfs.append(sub.iloc[10:-10])
                                        else:
                                            # if between 11–20, take roughly the middle chunk
                                            mid_start = max(1, total_balls // 2 - 2)
                                            mid_end = min(total_balls, total_balls // 2 + 2)
                                            phase_dfs.append(sub.iloc[mid_start:mid_end])

                                    # 🔴 Last 10 balls
                                    elif 'last' in sel:
                                        phase_dfs.append(sub.tail(10))

                                # ✅ Merge all filtered subsets back
                                if phase_dfs:
                                    df = pd.concat(phase_dfs, ignore_index=True)

                            except Exception as e:
                                print("⚠️ Error applying per-player ball-phase filter:", e)


                    except Exception as e:
                        print("Error filtering df for phase:", e)

            except Exception as e:
                print("Error loading data from DB:", e)

            # Generate Combined KPI Tables (Match + Total Summary in one)
            if df is not None and not df.empty:
                pvp_tables = generate_player_vs_player_table(
                    df,
                    selected_type,
                    batters=request.form.getlist("batters[]"),
                    bowlers=request.form.getlist("bowlers[]")
                )
                kpi_tables = generate_kpi_with_summary_tables(df, selected_type, player_vs_player_tables=pvp_tables)
            else:
                kpi_tables = {"No Data": "<p class='text-center text-slate-600 dark:text-zink-200'>No data found</p>"}

            # Fetch ball-by-ball details
            print("Fetching ball-by-ball details...")
            ball_by_ball_df = get_ball_by_ball_details(
                selected_matches,
                batters=request.form.getlist("batters[]"),
                bowlers=request.form.getlist("bowlers[]"),
                inning=selected_inning,
                session=selected_session,
                day=selected_day,
                from_over=from_over,
                to_over=to_over,
                player_id=selected_player,     # ✅ NEW (filters only selected player)
                view_type=selected_type        # ✅ NEW (batter/bowler mode)
            )

            # --- FILTER ball_by_ball_df BY SELECTED PLAYER ---
            # if selected_player and ball_by_ball_df is not None and not ball_by_ball_df.empty:
            #     try:
            #         pid_val = None
            #         pname = None
            #         try:
            #             pid_val = int(selected_player)
            #         except Exception:
            #             pid_val = selected_player
            #         for p in players:
            #             if str(p.get('id')) == str(selected_player):
            #                 pname = p.get('label', '').split(' (')[0]
            #                 break

            #         cols_id_bb = ['scrM_PlayMIdStriker', 'scrM_PlayMIdNonStriker', 'scrM_PlayMIdBowler']
            #         cols_name_bb = ['scrM_PlayMIdStrikerName', 'scrM_PlayMIdNonStrikerName', 'scrM_PlayMIdBowlerName']
            #         cond_bb = None
            #         for c in cols_id_bb:
            #             if c in ball_by_ball_df.columns:
            #                 if cond_bb is None:
            #                     cond_bb = (ball_by_ball_df[c] == pid_val)
            #                 else:
            #                     cond_bb |= (ball_by_ball_df[c] == pid_val)
            #         if pname:
            #             for c in cols_name_bb:
            #                 if c in ball_by_ball_df.columns:
            #                     if cond_bb is None:
            #                         cond_bb = (ball_by_ball_df[c] == pname)
            #                     else:
            #                         cond_bb |= (ball_by_ball_df[c] == pname)
            #         if cond_bb is not None:
            #             ball_by_ball_df = ball_by_ball_df[cond_bb]
            #             print(f"Filtered ball_by_ball_df rows for player {selected_player}: {len(ball_by_ball_df)}")
            #     except Exception as e:
            #         print('Error filtering ball_by_ball_df by selected_player:', e)
            # ✅ Filter videos by selected metric
            if selected_metric and not ball_by_ball_df.empty:
                selected_metric_lower = selected_metric.lower().strip()
                print(f"Filtering videos for metric: {selected_metric_lower}")

                metric_filters = {
                    "boundaries": (ball_by_ball_df["scrM_IsBoundry"] == 1) | (ball_by_ball_df["scrM_IsSixer"] == 1),
                    "sixes": (ball_by_ball_df["scrM_IsSixer"] == 1),
                    "fours": (ball_by_ball_df["scrM_IsBoundry"] == 1) & (ball_by_ball_df["scrM_IsSixer"] == 0),
                    "beaten": (ball_by_ball_df["scrM_IsBeaten"] == 1),
                    "uncomfort": (ball_by_ball_df["scrM_IsUncomfort"] == 1),
                    "wickets": (ball_by_ball_df["scrM_IsWicket"] == 1),
                    "dotballs": (ball_by_ball_df["scrM_BatsmanRuns"] == 0) & (ball_by_ball_df["scrM_IsValidBall"] == 1),
                    # Wide/No Ball support
                    "wide": (ball_by_ball_df.get("scrM_IsWideBall") == 1),
                    "wide ball": (ball_by_ball_df.get("scrM_IsWideBall") == 1),
                    "noball": (ball_by_ball_df.get("scrM_IsNoBall") == 1),
                    "no ball": (ball_by_ball_df.get("scrM_IsNoBall") == 1)
                }

                # Apply known filters
                for key, cond in metric_filters.items():
                    if key in selected_metric_lower:
                        ball_by_ball_df = ball_by_ball_df[cond]
                        break

                print(f"Filtered to {len(ball_by_ball_df)} rows for selected metric.")

            print(f"Ball-by-ball DataFrame loaded. Shape: {ball_by_ball_df.shape if not ball_by_ball_df.empty else 'Empty'}")

            # Apply filters to ball_by_ball_df
            if not ball_by_ball_df.empty:
                # Basic filters
                if selected_day:
                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_DayNo'] == int(selected_day)]
                if selected_inning:
                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_InningNo'] == int(selected_inning)]
                if selected_session:
                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_SessionNo'] == int(selected_session)]
                
                selected_batters_list = request.form.getlist("batters[]")
                selected_bowlers_list = request.form.getlist("bowlers[]")

                # ✅ Correct filtering should be done using NAME columns (not ID columns)
                if selected_type == "batter" and selected_batters_list:
                    if "scrM_PlayMIdStrikerName" in ball_by_ball_df.columns:
                        ball_by_ball_df = ball_by_ball_df[
                            ball_by_ball_df["scrM_PlayMIdStrikerName"].astype(str).isin(selected_batters_list)
                        ]

                if selected_type == "bowler" and selected_bowlers_list:
                    if "scrM_PlayMIdBowlerName" in ball_by_ball_df.columns:
                        ball_by_ball_df = ball_by_ball_df[
                            ball_by_ball_df["scrM_PlayMIdBowlerName"].astype(str).isin(selected_bowlers_list)
                        ]


                # Apply Over Filters (handles partial inputs too)
                if from_over or to_over:
                    from_over_val = int(from_over) if from_over else ball_by_ball_df['scrM_OverNo'].min()
                    to_over_val = int(to_over) if to_over else ball_by_ball_df['scrM_OverNo'].max()
                    ball_by_ball_df = ball_by_ball_df[
                        (ball_by_ball_df['scrM_OverNo'] >= from_over_val) &
                        (ball_by_ball_df['scrM_OverNo'] <= to_over_val)
                    ]

                # Phase filter (over-based)
                if selected_phase:
                    try:
                        sel_phase = str(selected_phase).strip().lower()
                        fmt = str(match_format).lower() if match_format else ""
                        pp_end, middle_end = 6, 15
                        if ('t10' in fmt) or (('10' in fmt) and ('t20' not in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                            pp_end, middle_end = 3, 7
                        elif ('t20' in fmt) or ('twenty' in fmt) or (('20' in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                            pp_end, middle_end = 6, 15
                        elif ('odi' in fmt) or ('one day' in fmt) or ('50' in fmt):
                            pp_end, middle_end = 10, 40
                        else:
                            try:
                                max_over = int(ball_by_ball_df['scrM_OverNo'].max())
                                if max_over == 50:
                                    pp_end, middle_end = 10, 40
                                elif max_over == 20:
                                    pp_end, middle_end = 6, 15
                                elif max_over == 10:
                                    pp_end, middle_end = 3, 7
                            except Exception:
                                pass

                        if sel_phase not in ('all', 'any', ''):
                            if 'power' in sel_phase:
                                ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_OverNo'] <= pp_end]
                            elif 'middle' in sel_phase:
                                ball_by_ball_df = ball_by_ball_df[
                                    (ball_by_ball_df['scrM_OverNo'] > pp_end) & 
                                    (ball_by_ball_df['scrM_OverNo'] <= middle_end)
                                ]
                            elif 'death' in sel_phase or 'slog' in sel_phase:
                                ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_OverNo'] > middle_end]
                    except Exception as e:
                        print("Error applying phase filter:", e)

                # --- BALL-PHASE (First 10 / Middle Balls / Last 10) ---
                if selected_ball_phase:
                    try:
                        sel_bp = str(selected_ball_phase).strip().lower()
                        fmt = str(match_format).lower() if match_format else ""
                        total_overs = infer_total_overs(fmt, df_for_infer=ball_by_ball_df)
                        total_balls = total_overs * 6

                        # create ball_index per row using OverNo and DelNo
                        try:
                            ball_by_ball_df['scrM_OverNo'] = ball_by_ball_df['scrM_OverNo'].astype(int)
                            ball_by_ball_df['scrM_DelNo'] = ball_by_ball_df['scrM_DelNo'].astype(int)
                        except Exception:
                            ball_by_ball_df['scrM_OverNo'] = pd.to_numeric(ball_by_ball_df['scrM_OverNo'], errors='coerce').fillna(0).astype(int)
                            ball_by_ball_df['scrM_DelNo'] = pd.to_numeric(ball_by_ball_df['scrM_DelNo'], errors='coerce').fillna(0).astype(int)

                        ball_by_ball_df['ball_index'] = (ball_by_ball_df['scrM_OverNo'] - 1) * 6 + ball_by_ball_df['scrM_DelNo']

                        # ✅ Apply per-batter or per-bowler ball phase (ball_by_ball_df)
                        if sel_bp and 'ball_index' in ball_by_ball_df.columns:
                            try:
                                key_col = 'scrM_PlayMIdStrikerName' if selected_type == 'batter' else 'scrM_PlayMIdBowlerName'
                                phase_dfs = []

                                for name, sub in ball_by_ball_df.groupby(key_col, group_keys=False):
                                    sub = sub.sort_values(['scrM_OverNo', 'scrM_DelNo']).reset_index(drop=True)
                                    total_balls = len(sub)

                                    if total_balls <= 10:
                                        phase_dfs.append(sub)
                                        continue

                                    if 'first' in sel_bp:
                                        phase_dfs.append(sub.head(10))
                                    elif 'last' in sel_bp:
                                        phase_dfs.append(sub.tail(10))
                                    elif 'middle' in sel_bp:
                                        if total_balls > 20:
                                            phase_dfs.append(sub.iloc[10:-10])
                                        else:
                                            mid_start = max(1, total_balls // 2 - 2)
                                            mid_end = min(total_balls, total_balls // 2 + 2)
                                            phase_dfs.append(sub.iloc[mid_start:mid_end])

                                if phase_dfs:
                                    ball_by_ball_df = pd.concat(phase_dfs, ignore_index=True)

                            except Exception as e:
                                print("⚠️ Error applying per-player ball phase (ball_by_ball_df):", e)

                        else:
                            # fallback by keywords
                            if '10' in sel_bp and 'first' in sel_bp:
                                ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['ball_index'] <= 10]
                    except Exception as e:
                        print("Error applying ball phase to ball_by_ball_df:", e)

                # --- FILTER ball_by_ball_df BY SELECTED TEAM (minimal change) ---
                if selected_team and not ball_by_ball_df.empty:
                    try:
                        st = selected_team
                        if str(st).isdigit():
                            tid = int(st)
                            if selected_type == "batter":
                                if 'scrM_tmMIdBatting' in ball_by_ball_df.columns:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBatting'] == tid]
                                elif 'scrM_tmMIdBattingName' in ball_by_ball_df.columns:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBattingName'] == str(tid)]
                            else:
                                if 'scrM_tmMIdBowling' in ball_by_ball_df.columns:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBowling'] == tid]
                                elif 'scrM_tmMIdBowlingName' in ball_by_ball_df.columns:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBowlingName'] == str(tid)]
                        else:
                            if selected_type == "batter":
                                if 'scrM_tmMIdBattingName' in ball_by_ball_df.columns:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBattingName'] == st]
                                else:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBatting'] == st]
                            else:
                                if 'scrM_tmMIdBowlingName' in ball_by_ball_df.columns:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBowlingName'] == st]
                                else:
                                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBowling'] == st]
                        print(f"Filtered ball_by_ball_df rows for team {selected_team}: {len(ball_by_ball_df)}")
                    except Exception as e:
                        print("Error filtering ball_by_ball_df by selected_team:", e)


            if not ball_by_ball_df.empty:
                print("Processing ball-by-ball DataFrame...")
                ball_by_ball_details = ball_by_ball_df.to_dict(orient='records')

                for ball in ball_by_ball_details:
                    ball['commentary'] = generate_commentary(ball)

                    for i in range(1, 7):
                        video_url_key = f'scrM_Video{i}URL'
                        if pd.isna(ball.get(video_url_key)):
                            ball[video_url_key] = None
                
                print("--- DEBUG START: Final ball_by_ball_details ---")
                import json
                print(json.dumps(ball_by_ball_details, indent=2))
                print("--- DEBUG END ---")

                # 🧠 Adjust reports based on selected_type
                # 🧠 Adjust reports based on selected_type
                if selected_type == "batter":
                    if ball_by_ball_df is not None and not ball_by_ball_df.empty:
                        # ✅ Normalize metric label for consistency
                        if selected_metric and selected_metric.lower().strip() in ["wicket", "wickets"]:
                            selected_metric = "Wicket"

                        # ✅ Check if single match is selected
                        is_single_match = len(selected_matches) == 1 if selected_matches else False
                        line_length_report = generate_line_length_report(
                            ball_by_ball_df,
                            selected_metric=selected_metric,
                            is_single_match=is_single_match,
                            selected_type=selected_type
                        )

                    else:
                        print("⚠️ No data found after filters — retaining last heatmap.")
                        if 'line_length_report' not in locals() or line_length_report is None:
                            line_length_report = {'heatmap_data': {}, 'totals': {}, 'table_data': [], 'pitch_points': []}

                    areawise_report = generate_areawise_report(ball_by_ball_df)
                    shottype_report = generate_shottype_report(ball_by_ball_df)
                    deliverytype_report = generate_deliverytype_report(ball_by_ball_df)


                else:
                    # 🆕 Bowler perspective (runs conceded, delivery focus)
                    try:
                        # Ensure bowler columns exist
                        if not ball_by_ball_df.empty:
                            # Create copies for clarity
                            df_bowl = ball_by_ball_df.copy()

                            # For bowler perspective, only rename player/team columns, NOT pitch columns
                            df_bowl["scrM_PlayMIdStrikerName"] = df_bowl["scrM_PlayMIdBowlerName"]
                            df_bowl["scrM_tmMIdBattingName"] = df_bowl["scrM_tmMIdBowlingName"]
                            df_bowl["scrM_BatPitchArea_zName"] = df_bowl["scrM_PitchArea_zName"]
                            # DO NOT overwrite pitch columns, keep scrM_PitchX and scrM_PitchY as is

                            # ✅ Generate bowler-based reports
                            # ✅ Check if single match is selected
                            is_single_match = len(selected_matches) == 1 if selected_matches else False
                            line_length_report = generate_line_length_report(
                                df_bowl,
                                selected_metric=selected_metric,
                                is_single_match=is_single_match,
                                selected_type="bowler"
                            )
                            areawise_report = generate_areawise_report(df_bowl)
                            shottype_report = generate_shottype_report(df_bowl)
                            deliverytype_report = generate_deliverytype_report(df_bowl)
                        else:
                            line_length_report = areawise_report = shottype_report = deliverytype_report = None
                    except Exception as e:
                        print("❌ Bowler report generation error:", e)


        except Exception as e:
            print("Error in processing player data:", e)

    # Debug
    if request.method == "POST":
        print("Line & Length Report Data:", line_length_report)
        print("📥 Filter Submission:", {
            "tournament": selected_tournament,
            "team": selected_team,
            "matches": selected_matches,
            "day": selected_day,
            "inning": selected_inning,
            "session": selected_session,
            "phase": selected_phase,
            "from_over": from_over,
            "to_over": to_over,
            "type": selected_type,
            "ball_phase": selected_ball_phase,
            "batters": request.form.getlist("batters[]"),
            "bowlers": request.form.getlist("bowlers[]"),
        })

    # ✅ update global cache for PDF export
    global player_kpi_cache
    player_kpi_cache = kpi_tables




    
    return render_template(
        "apps/apps-chat-1.html",
        tournaments=tournaments,
        teams=teams,
        matches=matches,
        batters=batters,
        bowlers=bowlers,
        days=days,
        innings=innings,
        sessions=sessions,
        match_format=match_format,
        selected_tournament=selected_tournament,
        selected_team=selected_team,
        selected_matches=selected_matches,
        selected_day=selected_day,
        selected_inning=selected_inning,
        selected_session=selected_session,
        selected_phase=selected_phase,
        from_over=from_over,
        to_over=to_over,
        selected_type=selected_type,
        selected_ball_phase=selected_ball_phase,
        kpi_tables=kpi_tables,
        ball_by_ball_details=ball_by_ball_details,
        selected_metric=selected_metric,
        areawise_report=areawise_report,
        shottype_report=shottype_report,
        deliverytype_report=deliverytype_report,
        radar_stats=radar_stats,
        radar_labels=radar_labels,
        radar_breakdown=radar_breakdown,
        ball_data=ball_by_ball_details if ball_by_ball_details else [],
        line_length_report=line_length_report if line_length_report else {"heatmap_data": {}},
        display_selected_batters=display_selected_batters if 'display_selected_batters' in locals() else [],
        display_selected_bowlers=display_selected_bowlers if 'display_selected_bowlers' in locals() else [],
        # pass multi-select placeholders and flag for this variant
        selected_tournaments=selected_tournaments,
        selected_teams=selected_teams,
        is_player_analysis_1=True,
        players=players,

    )

@apps.route('/apps/apps-chat', methods=['GET', 'POST'])
@login_required
def advanced_filters():
    # 🆕 Filter tournaments by logged-in user's association
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    tournaments = get_all_tournaments(association_id)

    selected_tournament = request.form.get("tournament") if request.method == "POST" else None
    selected_team = request.form.get("team") if request.method == "POST" else None
    selected_matches = request.form.getlist("matches[]") if request.method == "POST" else []

    # 🆕 Handle 'All' selection
    if 'All' in selected_matches and selected_team and selected_tournament:
        all_matches = get_matches_by_team(selected_team, selected_tournament)

        # ✅ If your function returns dict/list like [{"id":880,"label":"MI vs CSK"}]
        if isinstance(all_matches, list) and len(all_matches) > 0 and isinstance(all_matches[0], dict):
            selected_matches = [str(m["id"]) for m in all_matches]
        else:
            selected_matches = [str(x) for x in all_matches]


    # 🧹 Sanitize multiday filter inputs
    def sanitize_int_field(value):
        try:
            if not value or "select" in str(value).lower():
                return None
            return int(value)
        except Exception:
            return None

    selected_day_raw = request.form.get("day") or request.args.get("day")
    selected_inning_raw = request.form.get("inning") or request.args.get("inning")
    selected_session_raw = request.form.get("session") or request.args.get("session")

    selected_day = sanitize_int_field(selected_day_raw)
    selected_inning = sanitize_int_field(selected_inning_raw)
    selected_session = sanitize_int_field(selected_session_raw)

    selected_phase = request.form.get("phase") if request.method == "POST" else None
    from_over = request.form.get("from_over") if request.method == "POST" else None
    to_over = request.form.get("to_over") if request.method == "POST" else None
    selected_type = request.form.get("type") if request.method == "POST" else "batter"
    selected_ball_phase = request.form.get("ball_phase") if request.method == "POST" else None
    selected_metric = request.form.get("metric") if request.method == "POST" else None


    match_format = None
    # 🆕 Default radar vars
    radar_stats, radar_labels, radar_breakdown = None, None, None

    if selected_tournament:
        try:
            format_result = get_match_format_by_tournament(selected_tournament)
            match_format = format_result.lower() if format_result else None
        except Exception as e:
            print("Failed to get match format:", e)

    teams = get_teams_by_tournament(selected_tournament) if selected_tournament else []
    matches = get_matches_by_team(selected_team, selected_tournament) if selected_team else []


    days, innings, sessions = [], [], []
    batters, bowlers = [], []
    kpi_tables = {}
    ball_by_ball_details = []
    line_length_report = None
    areawise_report = None
    shottype_report = None
    deliverytype_report = None

    # Helper: determine total overs from match_format or data fallback
    def infer_total_overs(fmt, df_for_infer=None):
        try:
            fmt = (fmt or "").lower()
            if 't10' in fmt or ('10' in fmt and 't20' not in fmt and '50' not in fmt and 'odi' not in fmt):
                return 10
            if 't20' in fmt or 'twenty' in fmt or (('20' in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                return 20
            if 'odi' in fmt or 'one day' in fmt or '50' in fmt:
                return 50
            # fallback: infer from data passed (ball_by_ball or df)
            if df_for_infer is not None and not df_for_infer.empty:
                # compute max over number present in data
                if 'scrM_OverNo' in df_for_infer.columns:
                    try:
                        max_over = int(df_for_infer['scrM_OverNo'].max())
                        if max_over in (10, 20, 50):
                            return max_over
                    except Exception:
                        pass
            # default fallback: assume 20
            return 20
        except Exception:
            return 20

    if selected_matches:
        print("Entered 'if selected_matches' block.")
        try:
            # Filters
            days, innings, sessions = get_days_innings_sessions_by_matches(selected_matches)

            # Player Lists
            batters, bowlers = get_players_by_match(
                selected_matches,
                day=selected_day,
                inning=selected_inning,
                session=selected_session
            )

            # ✅ Enhance dropdown labels: show hand type for batters and bowling skill for bowlers (for MySQL schema)
            try:
                conn = get_connection()
                placeholders = ",".join(["%s"] * len(selected_matches))

                query = f"""
                    SELECT
                        s.scrM_PlayMIdStrikerName AS Batter,
                        MAX(NULLIF(TRIM(s.scrM_StrikerBatterSkill), '')) AS BatterSkill,
                        s.scrM_PlayMIdBowlerName AS Bowler,
                        MAX(NULLIF(TRIM(s.scrM_BowlerSkill), '')) AS BowlerSkill
                    FROM tblscoremaster s
                    WHERE s.scrM_MchMId IN ({placeholders})
                    AND s.scrM_IsValidBall = 1
                    GROUP BY s.scrM_PlayMIdStrikerName, s.scrM_PlayMIdBowlerName
                """
                player_df = pd.read_sql(query, conn, params=tuple(selected_matches))

                conn.close()

                if not player_df.empty:
                    # Normalize skills
                    for col in ['BatterSkill', 'BowlerSkill']:
                        player_df[col] = (
                            player_df[col]
                            .fillna('')
                            .astype(str)
                            .str.strip()
                            .str.upper()
                        )
                        # Extract only short codes (e.g., RHB)
                        player_df[col] = player_df[col].str.extract(r'\(([A-Z]+)\)')[0].fillna(player_df[col])

                    # Build display labels
                    player_df['display_batter'] = player_df.apply(
                        lambda x: f"{x['Batter']} ({x['BatterSkill']})" if x['BatterSkill'] else x['Batter'],
                        axis=1
                    )
                    player_df['display_bowler'] = player_df.apply(
                        lambda x: f"{x['Bowler']} ({x['BowlerSkill']})" if x['BowlerSkill'] else x['Bowler'],
                        axis=1
                    )

                    batter_skill_map = dict(zip(player_df['Batter'], player_df['display_batter']))
                    bowler_skill_map = dict(zip(player_df['Bowler'], player_df['display_bowler']))

                    batters = [batter_skill_map.get(b, b) for b in batters]
                    bowlers = [bowler_skill_map.get(b, b) for b in bowlers]

            except Exception as e:
                print("⚠️ Error enriching dropdowns with skill info:", e)

            # ✅ Handle “All” filters for Batters and Bowlers (ID Safe)
            # ✅ Handle “All” filters for Batters and Bowlers (ID Safe)
            try:
                selected_batters = request.form.getlist("batters[]")   # ✅ ids or All tokens
                selected_bowlers = request.form.getlist("bowlers[]")   # ✅ ids or All tokens

                # ✅ UI should show only what user selected
                display_selected_batters = selected_batters.copy()
                display_selected_bowlers = selected_bowlers.copy()

                all_batter_ids = [str(p["id"]) for p in batters] if batters else []
                all_bowler_ids = [str(p["id"]) for p in bowlers] if bowlers else []

                # ✅ Batter internal ids
                internal_batters = []

                if any(str(opt).startswith("All") for opt in selected_batters):
                    if "All" in selected_batters:
                        internal_batters = all_batter_ids
                        display_selected_batters = ["All"]

                    elif "All (RHB)" in selected_batters:
                        internal_batters = [
                            str(p["id"]) for p in batters
                            if "(RHB)" in str(p.get("name", "")).upper()
                        ]
                        display_selected_batters = ["All (RHB)"]

                    elif "All (LHB)" in selected_batters:
                        internal_batters = [
                            str(p["id"]) for p in batters
                            if "(LHB)" in str(p.get("name", "")).upper()
                        ]
                        display_selected_batters = ["All (LHB)"]

                else:
                    # ✅ Only keep selected ids (NO auto select all)
                    internal_batters = [str(x) for x in selected_batters if str(x).isdigit()]

                # ✅ Bowler internal ids
                internal_bowlers = []

                if any(str(opt).startswith("All") for opt in selected_bowlers):
                    if "All" in selected_bowlers:
                        internal_bowlers = all_bowler_ids
                        display_selected_bowlers = ["All"]

                    else:
                        selected_types = [s for s in selected_bowlers if str(s).startswith("All (")]
                        skills = [s.split("(")[1].replace(")", "").strip().upper() for s in selected_types]

                        internal_bowlers = [
                            str(p["id"]) for p in bowlers
                            if any(f"({skill})" in str(p.get("name", "")).upper() for skill in skills)
                        ]
                        display_selected_bowlers = selected_types

                else:
                    # ✅ Only keep selected ids (NO auto select all)
                    internal_bowlers = [str(x) for x in selected_bowlers if str(x).isdigit()]

                # ✅ Apply internally for DB queries ONLY (do not affect UI selected state)
                request.form = request.form.copy()
                request.form.setlist("batters[]", internal_batters)
                request.form.setlist("bowlers[]", internal_bowlers)

            except Exception as e:
                print("⚠️ Error applying 'All' filter logic:", e)
                display_selected_batters = []
                display_selected_bowlers = []





            
            df = None
            try:
                conn = get_connection()
                
                df = get_filtered_score_data(
                    conn,
                    selected_matches,
                    day=selected_day,
                    inning=selected_inning,
                    session=selected_session,
                    phase=selected_phase,
                    from_over=from_over,
                    to_over=to_over,
                    type=selected_type,
                    batters=request.form.getlist("batters[]"),
                    bowlers=request.form.getlist("bowlers[]")
                )
                conn.close()
                print(f"Data rows {len(df)} ")
                # --- FILTER df BY SELECTED TEAM (minimal change) ---
                # --- ✅ FILTER df BY SELECTED TEAM ID ---
                if selected_team and df is not None and not df.empty:
                    try:
                        team_id = str(selected_team).strip()

                        if selected_type == "batter":
                            if "scrM_tmMIdBatting" in df.columns:
                                df = df[df["scrM_tmMIdBatting"].astype(str).str.strip() == team_id]
                        else:
                            if "scrM_tmMIdBowling" in df.columns:
                                df = df[df["scrM_tmMIdBowling"].astype(str).str.strip() == team_id]

                        print(f"✅ Filtered df rows for team {team_id}: {len(df)}")

                    except Exception as e:
                        print("❌ Error filtering df by selected_team:", e)


                # --- ✅ Apply PHASE filter to df (for Player vs Player consistency) ---
                if df is not None and not df.empty:
                    try:
                        # Normalize
                        sel_phase = str(selected_phase).strip().lower() if selected_phase else ""
                        sel_ball_phase = str(selected_ball_phase).strip().lower() if selected_ball_phase else ""
                        fmt = str(match_format).lower() if match_format else ""
                        total_overs = infer_total_overs(fmt, df_for_infer=df)
                        total_balls = total_overs * 6

                        # Ensure OverNo and DelNo exist and create ball_index
                        if 'scrM_OverNo' in df.columns and 'scrM_DelNo' in df.columns:
                            # safe convert
                            try:
                                df['scrM_OverNo'] = df['scrM_OverNo'].astype(int)
                                df['scrM_DelNo'] = df['scrM_DelNo'].astype(int)
                            except Exception:
                                df['scrM_OverNo'] = pd.to_numeric(df['scrM_OverNo'], errors='coerce').fillna(0).astype(int)
                                df['scrM_DelNo'] = pd.to_numeric(df['scrM_DelNo'], errors='coerce').fillna(0).astype(int)

                            df['ball_index'] = (df['scrM_OverNo'] - 1) * 6 + df['scrM_DelNo']
                        else:
                            df['ball_index'] = None

                        # Over-based phase filter (Powerplay/Middle/Death) — if selected_phase present
                        if sel_phase:
                            try:
                                pp_end, middle_end = 6, 15  # default for T20
                                if ('t10' in fmt) or (('10' in fmt) and ('t20' not in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                                    pp_end, middle_end = 3, 7
                                elif ('t20' in fmt) or ('twenty' in fmt) or (('20' in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                                    pp_end, middle_end = 6, 15
                                elif ('odi' in fmt) or ('one day' in fmt) or ('50' in fmt):
                                    pp_end, middle_end = 10, 40
                                else:
                                    # infer from max over in df
                                    try:
                                        max_over = int(df['scrM_OverNo'].max())
                                        if max_over == 50:
                                            pp_end, middle_end = 10, 40
                                        elif max_over == 20:
                                            pp_end, middle_end = 6, 15
                                        elif max_over == 10:
                                            pp_end, middle_end = 3, 7
                                    except Exception:
                                        pass

                                if sel_phase not in ('all', ''):
                                    if 'power' in sel_phase:
                                        df = df[df['scrM_OverNo'] <= pp_end]
                                    elif 'middle' in sel_phase:
                                        df = df[(df['scrM_OverNo'] > pp_end) & (df['scrM_OverNo'] <= middle_end)]
                                    elif 'death' in sel_phase or 'slog' in sel_phase:
                                        df = df[df['scrM_OverNo'] > middle_end]
                            except Exception as e:
                                print("Error filtering df for phase:", e)

                        # Ball-phase filtering (First 10 / Middle Balls / Last 10) applied on ball_index (per innings)
                        # ✅ Apply per-batter or per-bowler ball-phase filtering (consistent with offline logic)
                        if sel_ball_phase and 'ball_index' in df.columns:
                            try:
                                key_col = 'scrM_PlayMIdStrikerName' if selected_type == 'batter' else 'scrM_PlayMIdBowlerName'
                                phase_dfs = []

                                for name, sub in df.groupby(key_col, group_keys=False):
                                    sub = sub.sort_values(['scrM_OverNo', 'scrM_DelNo']).reset_index(drop=True)
                                    total_balls = len(sub)

                                    # If player bowled/faced ≤10 balls, include all
                                    if total_balls <= 10:
                                        phase_dfs.append(sub)
                                        continue

                                    sel = sel_ball_phase.lower().strip()

                                    # 🟢 First 10 balls
                                    if 'first' in sel:
                                        phase_dfs.append(sub.head(10))

                                    # 🟠 Middle Balls (exclude first 10 and last 10)
                                    elif 'middle' in sel:
                                        if total_balls > 20:
                                            phase_dfs.append(sub.iloc[10:-10])
                                        else:
                                            # if between 11–20, take roughly the middle chunk
                                            mid_start = max(1, total_balls // 2 - 2)
                                            mid_end = min(total_balls, total_balls // 2 + 2)
                                            phase_dfs.append(sub.iloc[mid_start:mid_end])

                                    # 🔴 Last 10 balls
                                    elif 'last' in sel:
                                        phase_dfs.append(sub.tail(10))

                                # ✅ Merge all filtered subsets back
                                if phase_dfs:
                                    df = pd.concat(phase_dfs, ignore_index=True)

                            except Exception as e:
                                print("⚠️ Error applying per-player ball-phase filter:", e)


                    except Exception as e:
                        print("Error filtering df for phase:", e)

            except Exception as e:
                print("Error loading data from DB:", e)

            # ✅ Convert selected batter/bowler IDs → names for reports that need names
            selected_batter_ids = request.form.getlist("batters[]")
            selected_bowler_ids = request.form.getlist("bowlers[]")

            batter_id_to_name = {str(p["id"]): p["name"] for p in batters} if batters else {}
            bowler_id_to_name = {str(p["id"]): p["name"] for p in bowlers} if bowlers else {}

            selected_batter_names = [
                batter_id_to_name.get(str(i)) for i in selected_batter_ids
                if batter_id_to_name.get(str(i))
            ]

            selected_bowler_names = [
                bowler_id_to_name.get(str(i)) for i in selected_bowler_ids
                if bowler_id_to_name.get(str(i))
            ]

            print("✅ Selected Batter Names:", selected_batter_names)
            print("✅ Selected Bowler Names:", selected_bowler_names)


            # Generate Combined KPI Tables (Match + Total Summary in one)
            if df is not None and not df.empty:

                # ✅ Get selected IDs from form
                selected_batter_ids = request.form.getlist("batters[]")
                selected_bowler_ids = request.form.getlist("bowlers[]")

                # ✅ Remove "All" / invalid values safely
                selected_batter_ids = [x for x in selected_batter_ids if str(x).isdigit()]
                selected_bowler_ids = [x for x in selected_bowler_ids if str(x).isdigit()]

                # ✅ Convert batter IDs -> batter Names
                selected_batter_names = []
                if selected_batter_ids and "scrM_PlayMIdStriker" in df.columns:
                    selected_batter_names = (
                        df[df["scrM_PlayMIdStriker"].astype(str).isin(selected_batter_ids)]
                        ["scrM_PlayMIdStrikerName"]
                        .dropna()
                        .unique()
                        .tolist()
                    )

                # ✅ Convert bowler IDs -> bowler Names
                selected_bowler_names = []
                if selected_bowler_ids and "scrM_PlayMIdBowler" in df.columns:
                    selected_bowler_names = (
                        df[df["scrM_PlayMIdBowler"].astype(str).isin(selected_bowler_ids)]
                        ["scrM_PlayMIdBowlerName"]
                        .dropna()
                        .unique()
                        .tolist()
                    )

                # ✅ Generate Player vs Player tables
                pvp_tables = generate_player_vs_player_table(
                    df,
                    selected_type,
                    batters=selected_batter_names,
                    bowlers=selected_bowler_names
                )

                # ✅ Generate KPI tables
                kpi_tables = generate_kpi_with_summary_tables(
                    df,
                    selected_type,
                    player_vs_player_tables=pvp_tables
                )

                # ✅ Fallback in case KPI is empty (avoid blank UI)
                if not kpi_tables:
                    kpi_tables = {
                        "No Data": "<p class='text-center text-slate-600 dark:text-zink-200'>No KPI data found</p>"
                    }

            else:
                kpi_tables = {
                    "No Data": "<p class='text-center text-slate-600 dark:text-zink-200'>No data found</p>"
                }


            # Fetch ball-by-ball details
            print("Fetching ball-by-ball details...")
            ball_by_ball_df = get_ball_by_ball_details(
                selected_matches,
                batters=request.form.getlist("batters[]"),
                bowlers=request.form.getlist("bowlers[]"),
                inning=selected_inning,
                session=selected_session,
                day=selected_day,
                from_over=from_over,
                to_over=to_over,
                view_type=selected_type
            )

            # ✅ Filter videos by selected metric
            if selected_metric and not ball_by_ball_df.empty:
                selected_metric_lower = selected_metric.lower().strip()
                print(f"Filtering videos for metric: {selected_metric_lower}")

                metric_filters = {
                    "boundaries": (ball_by_ball_df["scrM_IsBoundry"] == 1) | (ball_by_ball_df["scrM_IsSixer"] == 1),
                    "sixes": (ball_by_ball_df["scrM_IsSixer"] == 1),
                    "fours": (ball_by_ball_df["scrM_IsBoundry"] == 1) & (ball_by_ball_df["scrM_IsSixer"] == 0),
                    "beaten": (ball_by_ball_df["scrM_IsBeaten"] == 1),
                    "uncomfort": (ball_by_ball_df["scrM_IsUncomfort"] == 1),
                    "wickets": (ball_by_ball_df["scrM_IsWicket"] == 1),
                    "dotballs": (ball_by_ball_df["scrM_BatsmanRuns"] == 0) & (ball_by_ball_df["scrM_IsValidBall"] == 1),
                    # Wide/No Ball support
                    "wide": (ball_by_ball_df.get("scrM_IsWideBall") == 1),
                    "wide ball": (ball_by_ball_df.get("scrM_IsWideBall") == 1),
                    "noball": (ball_by_ball_df.get("scrM_IsNoBall") == 1),
                    "no ball": (ball_by_ball_df.get("scrM_IsNoBall") == 1)
                }

                # Apply known filters
                for key, cond in metric_filters.items():
                    if key in selected_metric_lower:
                        ball_by_ball_df = ball_by_ball_df[cond]
                        break

                print(f"Filtered to {len(ball_by_ball_df)} rows for selected metric.")

            print(f"Ball-by-ball DataFrame loaded. Shape: {ball_by_ball_df.shape if not ball_by_ball_df.empty else 'Empty'}")

            # Apply filters to ball_by_ball_df
            if not ball_by_ball_df.empty:
                # Basic filters
                if selected_day:
                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_DayNo'] == int(selected_day)]
                if selected_inning:
                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_InningNo'] == int(selected_inning)]
                if selected_session:
                    ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_SessionNo'] == int(selected_session)]
                
                selected_batters_list = request.form.getlist("batters[]")
                if selected_batters_list:
                    ball_by_ball_df = ball_by_ball_df[
                        ball_by_ball_df['scrM_PlayMIdStriker'].astype(str).isin([str(x) for x in selected_batters_list])
                    ]

                selected_bowlers_list = request.form.getlist("bowlers[]")
                if selected_bowlers_list:
                    ball_by_ball_df = ball_by_ball_df[
                        ball_by_ball_df['scrM_PlayMIdBowler'].astype(str).isin([str(x) for x in selected_bowlers_list])
                    ]


                # Apply Over Filters (handles partial inputs too)
                if from_over or to_over:
                    from_over_val = int(from_over) if from_over else ball_by_ball_df['scrM_OverNo'].min()
                    to_over_val = int(to_over) if to_over else ball_by_ball_df['scrM_OverNo'].max()
                    ball_by_ball_df = ball_by_ball_df[
                        (ball_by_ball_df['scrM_OverNo'] >= from_over_val) &
                        (ball_by_ball_df['scrM_OverNo'] <= to_over_val)
                    ]

                # Phase filter (over-based)
                if selected_phase:
                    try:
                        sel_phase = str(selected_phase).strip().lower()
                        fmt = str(match_format).lower() if match_format else ""
                        pp_end, middle_end = 6, 15
                        if ('t10' in fmt) or (('10' in fmt) and ('t20' not in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                            pp_end, middle_end = 3, 7
                        elif ('t20' in fmt) or ('twenty' in fmt) or (('20' in fmt) and ('50' not in fmt) and ('odi' not in fmt)):
                            pp_end, middle_end = 6, 15
                        elif ('odi' in fmt) or ('one day' in fmt) or ('50' in fmt):
                            pp_end, middle_end = 10, 40
                        else:
                            try:
                                max_over = int(ball_by_ball_df['scrM_OverNo'].max())
                                if max_over == 50:
                                    pp_end, middle_end = 10, 40
                                elif max_over == 20:
                                    pp_end, middle_end = 6, 15
                                elif max_over == 10:
                                    pp_end, middle_end = 3, 7
                            except Exception:
                                pass

                        if sel_phase not in ('all', 'any', ''):
                            if 'power' in sel_phase:
                                ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_OverNo'] <= pp_end]
                            elif 'middle' in sel_phase:
                                ball_by_ball_df = ball_by_ball_df[
                                    (ball_by_ball_df['scrM_OverNo'] > pp_end) & 
                                    (ball_by_ball_df['scrM_OverNo'] <= middle_end)
                                ]
                            elif 'death' in sel_phase or 'slog' in sel_phase:
                                ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_OverNo'] > middle_end]
                    except Exception as e:
                        print("Error applying phase filter:", e)

                # --- BALL-PHASE (First 10 / Middle Balls / Last 10) ---
                if selected_ball_phase:
                    try:
                        sel_bp = str(selected_ball_phase).strip().lower()
                        fmt = str(match_format).lower() if match_format else ""
                        total_overs = infer_total_overs(fmt, df_for_infer=ball_by_ball_df)
                        total_balls = total_overs * 6

                        # create ball_index per row using OverNo and DelNo
                        try:
                            ball_by_ball_df['scrM_OverNo'] = ball_by_ball_df['scrM_OverNo'].astype(int)
                            ball_by_ball_df['scrM_DelNo'] = ball_by_ball_df['scrM_DelNo'].astype(int)
                        except Exception:
                            ball_by_ball_df['scrM_OverNo'] = pd.to_numeric(ball_by_ball_df['scrM_OverNo'], errors='coerce').fillna(0).astype(int)
                            ball_by_ball_df['scrM_DelNo'] = pd.to_numeric(ball_by_ball_df['scrM_DelNo'], errors='coerce').fillna(0).astype(int)

                        ball_by_ball_df['ball_index'] = (ball_by_ball_df['scrM_OverNo'] - 1) * 6 + ball_by_ball_df['scrM_DelNo']

                        # ✅ Apply per-batter or per-bowler ball phase (ball_by_ball_df)
                        if sel_bp and 'ball_index' in ball_by_ball_df.columns:
                            try:
                                key_col = 'scrM_PlayMIdStrikerName' if selected_type == 'batter' else 'scrM_PlayMIdBowlerName'
                                phase_dfs = []

                                for name, sub in ball_by_ball_df.groupby(key_col, group_keys=False):
                                    sub = sub.sort_values(['scrM_OverNo', 'scrM_DelNo']).reset_index(drop=True)
                                    total_balls = len(sub)

                                    if total_balls <= 10:
                                        phase_dfs.append(sub)
                                        continue

                                    if 'first' in sel_bp:
                                        phase_dfs.append(sub.head(10))
                                    elif 'last' in sel_bp:
                                        phase_dfs.append(sub.tail(10))
                                    elif 'middle' in sel_bp:
                                        if total_balls > 20:
                                            phase_dfs.append(sub.iloc[10:-10])
                                        else:
                                            mid_start = max(1, total_balls // 2 - 2)
                                            mid_end = min(total_balls, total_balls // 2 + 2)
                                            phase_dfs.append(sub.iloc[mid_start:mid_end])

                                if phase_dfs:
                                    ball_by_ball_df = pd.concat(phase_dfs, ignore_index=True)

                            except Exception as e:
                                print("⚠️ Error applying per-player ball phase (ball_by_ball_df):", e)

                        else:
                            # fallback by keywords
                            if '10' in sel_bp and 'first' in sel_bp:
                                ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['ball_index'] <= 10]
                    except Exception as e:
                        print("Error applying ball phase to ball_by_ball_df:", e)

                # --- FILTER ball_by_ball_df BY SELECTED TEAM (minimal change) ---
                # --- ✅ FILTER ball_by_ball_df BY SELECTED TEAM ID ---
                # ✅ FILTER ball_by_ball_df BY TEAM ID
                if selected_team and not ball_by_ball_df.empty:
                    try:
                        team_id = str(selected_team).strip()

                        if selected_type == "batter":
                            if "scrM_tmMIdBatting" in ball_by_ball_df.columns:
                                ball_by_ball_df = ball_by_ball_df[
                                    ball_by_ball_df["scrM_tmMIdBatting"].astype(str).str.strip() == team_id
                                ]
                        else:
                            if "scrM_tmMIdBowling" in ball_by_ball_df.columns:
                                ball_by_ball_df = ball_by_ball_df[
                                    ball_by_ball_df["scrM_tmMIdBowling"].astype(str).str.strip() == team_id
                                ]

                        print(f"✅ Filtered ball_by_ball_df rows for team {team_id}: {len(ball_by_ball_df)}")

                    except Exception as e:
                        print("❌ Error filtering ball_by_ball_df by selected_team:", e)




            if not ball_by_ball_df.empty:
                print("Processing ball-by-ball DataFrame...")
                ball_by_ball_details = ball_by_ball_df.to_dict(orient='records')

                for ball in ball_by_ball_details:
                    ball['commentary'] = generate_commentary(ball)

                    for i in range(1, 7):
                        video_url_key = f'scrM_Video{i}URL'
                        if pd.isna(ball.get(video_url_key)):
                            ball[video_url_key] = None
                
                print("--- DEBUG START: Final ball_by_ball_details ---")
                import json
                print(json.dumps(ball_by_ball_details, indent=2))
                print("--- DEBUG END ---")

                # 🧠 Adjust reports based on selected_type
                # 🧠 Adjust reports based on selected_type
                if selected_type == "batter":
                    if ball_by_ball_df is not None and not ball_by_ball_df.empty:
                        # ✅ Normalize metric label for consistency
                        if selected_metric and selected_metric.lower().strip() in ["wicket", "wickets"]:
                            selected_metric = "Wicket"

                        # ✅ Check if single match is selected
                        is_single_match = len(selected_matches) == 1 if selected_matches else False
                        line_length_report = generate_line_length_report(
                            ball_by_ball_df,
                            selected_metric=selected_metric,
                            is_single_match=is_single_match,
                            selected_type=selected_type
                        )

                    else:
                        print("⚠️ No data found after filters — retaining last heatmap.")
                        if 'line_length_report' not in locals() or line_length_report is None:
                            line_length_report = {'heatmap_data': {}, 'totals': {}, 'table_data': [], 'pitch_points': []}

                    areawise_report = generate_areawise_report(ball_by_ball_df)
                    shottype_report = generate_shottype_report(ball_by_ball_df)
                    deliverytype_report = generate_deliverytype_report(ball_by_ball_df)


                else:
                    # 🆕 Bowler perspective (runs conceded, delivery focus)
                    try:
                        if ball_by_ball_df is not None and not ball_by_ball_df.empty:

                            df_bowl = ball_by_ball_df.copy()

                            # ✅ Ensure scrM_Line & scrM_Length exist
                            if "scrM_Line" not in df_bowl.columns and "scrM_PitchXPos" in df_bowl.columns:
                                df_bowl["scrM_Line"] = df_bowl["scrM_PitchXPos"]

                            if "scrM_Length" not in df_bowl.columns and "scrM_PitchYPos" in df_bowl.columns:
                                df_bowl["scrM_Length"] = df_bowl["scrM_PitchYPos"]

                            # ✅ Bowler mode: make bowler behave like striker for reporting
                            if "scrM_PlayMIdBowlerName" in df_bowl.columns:
                                df_bowl["scrM_PlayMIdStrikerName"] = df_bowl["scrM_PlayMIdBowlerName"]

                            if "scrM_PlayMIdBowler" in df_bowl.columns:
                                df_bowl["scrM_PlayMIdStriker"] = df_bowl["scrM_PlayMIdBowler"]

                            # ✅ Team swap for bowler report
                            if "scrM_tmMIdBowlingName" in df_bowl.columns:
                                df_bowl["scrM_tmMIdBattingName"] = df_bowl["scrM_tmMIdBowlingName"]

                            if "scrM_tmMIdBowling" in df_bowl.columns:
                                df_bowl["scrM_tmMIdBatting"] = df_bowl["scrM_tmMIdBowling"]

                            # ✅ Use pitch area as batting pitch area (required by some reports)
                            if "scrM_PitchArea_zName" in df_bowl.columns:
                                df_bowl["scrM_BatPitchArea_zName"] = df_bowl["scrM_PitchArea_zName"]

                            # ✅ Check if single match is selected
                            is_single_match = len(selected_matches) == 1 if selected_matches else False

                            # ✅ IMPORTANT: Heatmap must use df_bowl (NOT df)
                            line_length_report = generate_line_length_report(
                                df_bowl,
                                selected_metric=selected_metric,
                                is_single_match=is_single_match,
                                selected_type="bowler"
                            )

                            areawise_report = generate_areawise_report(df_bowl)
                            shottype_report = generate_shottype_report(df_bowl)
                            deliverytype_report = generate_deliverytype_report(df_bowl)

                        else:
                            print("⚠️ No ball-by-ball data for bowler mode.")
                            line_length_report = {'heatmap_data': {}, 'totals': {}, 'table_data': [], 'pitch_points': []}
                            areawise_report = shottype_report = deliverytype_report = None

                    except Exception as e:
                        print("❌ Bowler report generation error:", e)


        except Exception as e:
            print("Error in processing player data:", e)

    # Debug
    if request.method == "POST":
        print("Line & Length Report Data:", line_length_report)
        print("📥 Filter Submission:", {
            "tournament": selected_tournament,
            "team": selected_team,
            "matches": selected_matches,
            "day": selected_day,
            "inning": selected_inning,
            "session": selected_session,
            "phase": selected_phase,
            "from_over": from_over,
            "to_over": to_over,
            "type": selected_type,
            "ball_phase": selected_ball_phase,
            "batters": request.form.getlist("batters[]"),
            "bowlers": request.form.getlist("bowlers[]"),
        })

    # ✅ update global cache for PDF export
    global player_kpi_cache
    player_kpi_cache = kpi_tables




    
    return render_template(
        "apps/apps-chat.html",
        tournaments=tournaments,
        teams=teams,
        matches=matches,
        batters=batters,
        bowlers=bowlers,
        days=days,
        innings=innings,
        sessions=sessions,
        match_format=match_format,
        selected_tournament=selected_tournament,
        selected_team=selected_team,
        selected_matches=selected_matches,
        selected_day=selected_day,
        selected_inning=selected_inning,
        selected_session=selected_session,
        selected_phase=selected_phase,
        from_over=from_over,
        to_over=to_over,
        selected_type=selected_type,
        selected_ball_phase=selected_ball_phase,
        kpi_tables=kpi_tables,
        ball_by_ball_details=ball_by_ball_details,
        selected_metric=selected_metric,
        areawise_report=areawise_report,
        shottype_report=shottype_report,
        deliverytype_report=deliverytype_report,
        radar_stats=radar_stats,
        radar_labels=radar_labels,
        radar_breakdown=radar_breakdown,
        ball_data=ball_by_ball_details if ball_by_ball_details else [],
        line_length_report=line_length_report if line_length_report else {"heatmap_data": {}},
        display_selected_batters=display_selected_batters if 'display_selected_batters' in locals() else [],
        display_selected_bowlers=display_selected_bowlers if 'display_selected_bowlers' in locals() else [],

    )

@apps.route("/apps/heatmap_matrix", methods=["POST"])
@login_required
def heatmap_matrix():
    """
    Returns server-calculated heatmap matrix + pitch_points list.
    Accepts advanced filters: matches, metric, team, batters, bowlers, inning,
    session, day, from_over, to_over, type (batter/bowler), phase, ball_phase.

    ✅ Updated Version:
    - Ensures pitch_points includes zone_key for every ball
    - Fixes selected_type logic (JSON request)
    - Keeps your All-match + skill + ball-phase logic intact
    - ✅ PitchPad run filter support (All / 0 / 1 / 2 / 3 / 4 / 6 / W)
    - ✅ NEW: Filters heatmap ONLY by selected Player (Player Analysis 1)
    """
    import re
    import pandas as pd

    try:
        # ---------------- Parse Input JSON ----------------
        data = request.get_json(force=True) or {}

        metric = (data.get("metric") or "").strip()
        matches = data.get("matches", []) or []
        team = data.get("team")
        view_type = (data.get("type") or "batter").strip()  # ✅ batter/bowler

        # ✅ NEW: Selected Player from Player Analysis 1 dropdown
        player = data.get("player")
        if player is not None:
            player = str(player).strip()

        # ✅ PitchPad filter (All/0/1/2/3/4/6/W)
        pitch_run_filter = str(data.get("pitch_run_filter", "all")).strip()

        def sanitize_field(val):
            try:
                if not val or "select" in str(val).lower():
                    return None
                return int(val)
            except Exception:
                return None

        day = sanitize_field(data.get("day"))
        inning = sanitize_field(data.get("inning"))
        session = sanitize_field(data.get("session"))
        phase = data.get("phase")
        from_over = data.get("from_over")
        to_over = data.get("to_over")

        batters = data.get("batters", []) or []
        bowlers = data.get("bowlers", []) or []
        ball_phase = data.get("ball_phase")

        # ---------------- Helpers ----------------
        def ensure_list(x):
            if x is None:
                return []
            if isinstance(x, str):
                return [x.strip()]
            try:
                return [str(i).strip() for i in list(x)]
            except Exception:
                return []

        def clean_list(lst):
            out = []
            for v in lst:
                if not v:
                    continue
                s = str(v).strip()
                if not s or s.lower() == "undefined":
                    continue
                out.append(s)
            return out

        matches = clean_list(ensure_list(matches))
        batters = clean_list(ensure_list(batters))
        bowlers = clean_list(ensure_list(bowlers))

        # Accept explicit quick-skill tokens passed separately (from client-side)
        explicit_bowler_skill = (data.get('bowler_skill') or data.get('bowlerSkill') or '')
        explicit_batter_skill = (data.get('batter_skill') or data.get('batterSkill') or '')

        if explicit_bowler_skill:
            explicit_bowler_skill = str(explicit_bowler_skill).strip()
        if explicit_batter_skill:
            explicit_batter_skill = str(explicit_batter_skill).strip()

        # If client sent tokens like 'Pace'/'Spin' inside bowlers list
        if bowlers:
            lower_vals = [str(x).strip().lower() for x in bowlers]
            for token in ['pace', 'spin']:
                if token in lower_vals:
                    explicit_bowler_skill = token
                    bowlers = []  # clear literal bowlers
                    break

        # If client sent tokens like 'RHB'/'LHB' inside batters list
        if batters:
            lower_vals_b = [str(x).strip().lower() for x in batters]
            for token in ['rhb', 'lhb']:
                if token in lower_vals_b:
                    explicit_batter_skill = token.upper()
                    batters = []
                    break

        # Detect “All” or “All (Skill)”
        def detect_all_and_skill(lst):
            has_all = False
            skill_token = None
            for t in lst:
                m = re.match(r'^\s*all\s*(?:\(\s*([^)]+?)\s*\))?\s*$', t, flags=re.I)
                if m:
                    has_all = True
                    if m.group(1):
                        skill_token = m.group(1).strip()
            return has_all, skill_token

        matches_has_all, _ = detect_all_and_skill(matches)
        batters_has_all, batters_skill = detect_all_and_skill(batters)
        bowlers_has_all, bowlers_skill = detect_all_and_skill(bowlers)

        # Prefer explicit skill if provided
        if explicit_bowler_skill:
            bowlers_skill = str(explicit_bowler_skill)
        if explicit_batter_skill:
            batters_skill = str(explicit_batter_skill)

        # Expand matches if “All”
        if matches_has_all or not matches:
            try:
                conn2 = get_connection()
                df_matches = pd.read_sql(
                    "SELECT mchM_Id FROM tblmatchmaster WHERE mchM_Id IS NOT NULL",
                    conn2,
                )
                matches = df_matches["mchM_Id"].astype(str).tolist() if not df_matches.empty else []

            except Exception:
                matches = []

        if isinstance(matches, str):
            matches = [matches]

        match_filter_applied = bool(matches)

        # If batters/bowlers had All -> clear them
        if batters_has_all:
            batters = []
        if bowlers_has_all:
            bowlers = []

        # ---------------- Fetch Data ----------------
        conn = get_connection()
        df = get_filtered_score_data(
            conn,
            matches if match_filter_applied else None,   # ✅ positional match_ids
            batters=batters,
            bowlers=bowlers,
            inning=inning,
            session=session,
            day=day,
            phase=phase,
            from_over=from_over,
            to_over=to_over,
            type=view_type,
            ball_phase=None,
        )
        conn.close()

        df_len = len(df) if df is not None else 0
        print(f"[DEBUG] heatmap_matrix after fetch: player={player}, matches={matches}, batters={batters}, bowlers={bowlers}, batters_skill={batters_skill}, bowlers_skill={bowlers_skill}, pitch_run_filter={pitch_run_filter}, df_rows={df_len}")

        if df is None or df.empty:
            return jsonify({"heatmap_data": {}, "totals": {}, "pitch_points": []})

        # ✅ NEW: Filter only selected player balls
        # ✅ NEW: Filter only selected player balls
        if player and player.lower() not in ("all", "select", "undefined"):
            try:
                player_str = str(player).strip()

                if view_type == "batter":
                    # by striker id
                    if player_str.isdigit() and "scrM_PlayMIdStriker" in df.columns:
                        df = df[df["scrM_PlayMIdStriker"].astype(str).str.strip() == player_str]

                    # by striker name
                    if df.empty and "scrM_PlayMIdStrikerName" in df.columns:
                        df = df[df["scrM_PlayMIdStrikerName"].astype(str).str.strip().str.lower() == player_str.lower()]

                else:
                    # by bowler id
                    if player_str.isdigit() and "scrM_PlayMIdBowler" in df.columns:
                        df = df[df["scrM_PlayMIdBowler"].astype(str).str.strip() == player_str]

                    # by bowler name
                    if df.empty and "scrM_PlayMIdBowlerName" in df.columns:
                        df = df[df["scrM_PlayMIdBowlerName"].astype(str).str.strip().str.lower() == player_str.lower()]

                print(f"✅ Player Filter applied => view_type={view_type}, player={player_str}, rows={len(df)}")

            except Exception as e:
                print("⚠️ Player filtering failed:", e)


        if df is None or df.empty:
            return jsonify({"heatmap_data": {}, "totals": {}, "pitch_points": []})

        # ---------------- Apply Ball Phase Filter (Python) ----------------
        def normalize_ball_phase(bp):
            if not bp:
                return None
            s = str(bp).strip().lower()
            if "first" in s:
                return "first"
            if "last" in s:
                return "last"
            if "middle" in s:
                return "middle"
            if "10" in s:
                return "first"
            return None

        bp_token = normalize_ball_phase(ball_phase)

        if bp_token and not df.empty:
            try:
                if "scrM_OverNo" in df.columns and "scrM_DelNo" in df.columns:
                    df["scrM_OverNo"] = pd.to_numeric(df["scrM_OverNo"], errors="coerce").fillna(0).astype(int)
                    df["scrM_DelNo"] = pd.to_numeric(df["scrM_DelNo"], errors="coerce").fillna(0).astype(int)

                key_col = "scrM_PlayMIdStrikerName" if view_type == "batter" else "scrM_PlayMIdBowlerName"

                if key_col in df.columns:
                    phase_dfs = []
                    for name, sub in df.groupby(key_col, group_keys=False):
                        sub = sub.sort_values(["scrM_OverNo", "scrM_DelNo"]).reset_index(drop=True)
                        total = len(sub)

                        if total <= 10:
                            phase_dfs.append(sub)
                            continue

                        if bp_token == "first":
                            phase_dfs.append(sub.head(10))
                        elif bp_token == "last":
                            phase_dfs.append(sub.tail(10))
                        elif bp_token == "middle":
                            if total > 20:
                                phase_dfs.append(sub.iloc[10:-10])
                            else:
                                mid_start = max(1, total // 2 - 2)
                                mid_end = min(total, total // 2 + 2)
                                phase_dfs.append(sub.iloc[mid_start:mid_end])

                    if phase_dfs:
                        df = pd.concat(phase_dfs, ignore_index=True)
                        print(f"✅ Ball Phase '{bp_token}' applied; rows={len(df)}")

            except Exception as e:
                print("⚠️ Ball Phase filtering failed:", e)

        # ---------------- Skill Filters ----------------
        if batters_skill:
            bs = str(batters_skill).strip().upper()
            cols = [c for c in df.columns if "batter" in c.lower() and "skill" in c.lower()]
            for fallback in ["scrM_StrikerBatterSkill", "scrM_BatsmanSkill", "scrM_PlayMIdStrikerSkill"]:
                if fallback in df.columns and fallback not in cols:
                    cols.insert(0, fallback)

            for col in cols:
                try:
                    df = df[df[col].astype(str).str.upper().str.contains(bs, na=False)]
                    break
                except Exception:
                    continue

        if bowlers_skill:
            bws = str(bowlers_skill).strip().upper()
            cols = [c for c in df.columns if "bowler" in c.lower() and "skill" in c.lower()]
            for fallback in ["scrM_BowlerSkill", "scrM_PlayMIdBowlerSkill"]:
                if fallback in df.columns and fallback not in cols:
                    cols.insert(0, fallback)

            # Special handling for high-level tokens 'PACE' / 'SPIN'
            if bws in ("PACE", "SPIN"):
                pace_keywords = ['FAST', 'FAST-MEDIUM', 'MEDIUM FAST', 'RAF', 'RAMF', 'LAF', 'LAMF', 'MF', 'F']
                spin_keywords = ['SPIN', 'OFF', 'LEG', 'LEFT-ARM', 'RIGHT-ARM', 'OFFBREAK', 'LEGBREAK', 'SLOW', 'CHINAMAN']
                kws = pace_keywords if bws == 'PACE' else spin_keywords
                try:
                    pattern = r"(?:" + r"|".join([re.escape(k.upper()) for k in kws]) + r")"
                    for col in cols:
                        try:
                            df = df[df[col].astype(str).str.upper().str.contains(pattern, na=False)]
                            break
                        except Exception:
                            continue
                except Exception:
                    for col in cols:
                        try:
                            df = df[df[col].astype(str).str.upper().str.contains(bws, na=False)]
                            break
                        except Exception:
                            continue
            else:
                for col in cols:
                    try:
                        df = df[df[col].astype(str).str.upper().str.contains(bws, na=False)]
                        break
                    except Exception:
                        continue

        # ---------------- Team Filter ----------------
        # ---------------- ✅ Team Filter (ID Safe + Name fallback) ----------------
        if team:
            try:
                team_val = str(team).strip()

                # ✅ if team id is coming (example "275")
                if team_val.isdigit():

                    if view_type == "batter":
                        if "scrM_tmMIdBatting" in df.columns:
                            df = df[df["scrM_tmMIdBatting"].astype(str).str.strip() == team_val]
                        elif "scrM_tmMIdBattingName" in df.columns:
                            # fallback if you stored id in name column (rare)
                            df = df[df["scrM_tmMIdBattingName"].astype(str).str.strip() == team_val]

                    else:
                        if "scrM_tmMIdBowling" in df.columns:
                            df = df[df["scrM_tmMIdBowling"].astype(str).str.strip() == team_val]
                        elif "scrM_tmMIdBowlingName" in df.columns:
                            df = df[df["scrM_tmMIdBowlingName"].astype(str).str.strip() == team_val]

                else:
                    # ✅ if team name is coming (example "HYD")
                    if view_type == "batter" and "scrM_tmMIdBattingName" in df.columns:
                        df = df[df["scrM_tmMIdBattingName"].astype(str).str.strip() == team_val]

                    elif view_type == "bowler" and "scrM_tmMIdBowlingName" in df.columns:
                        df = df[df["scrM_tmMIdBowlingName"].astype(str).str.strip() == team_val]

                print(f"✅ heatmap_matrix Team Filter applied => team={team_val}, view_type={view_type}, rows={len(df)}")

            except Exception as e:
                print("⚠️ Team filtering failed in heatmap_matrix:", e)


        # ✅ PitchPad Runs/Wicket Filter (must happen BEFORE heatmap generation)
        if pitch_run_filter and pitch_run_filter.lower() != "all":

            if "scrM_IsWicket" in df.columns:
                df["scrM_IsWicket"] = pd.to_numeric(df["scrM_IsWicket"], errors="coerce").fillna(0).astype(int)

            if "scrM_BatsmanRuns" in df.columns:
                df["scrM_BatsmanRuns"] = pd.to_numeric(df["scrM_BatsmanRuns"], errors="coerce").fillna(0).astype(int)

            if str(pitch_run_filter).upper() == "W":
                if "scrM_IsWicket" in df.columns:
                    df = df[df["scrM_IsWicket"] == 1]
            else:
                try:
                    run_val = int(str(pitch_run_filter).strip())
                except Exception:
                    run_val = None

                if run_val is not None and "scrM_BatsmanRuns" in df.columns:
                    if "scrM_IsWicket" in df.columns:
                        df = df[(df["scrM_IsWicket"] != 1) & (df["scrM_BatsmanRuns"] == run_val)]
                    else:
                        df = df[df["scrM_BatsmanRuns"] == run_val]

        if df.empty:
            return jsonify({"heatmap_data": {}, "totals": {}, "pitch_points": []})

        # ---------------- Generate Heatmap ----------------
        is_single_match = len(matches) == 1 if matches else False

        # ✅ FIXED: selected_type must come from JSON view_type (NOT request.form)
        selected_type = view_type

        hm = (
            generate_heatmap_matrix(df, selected_metric=metric, is_single_match=is_single_match, selected_type=selected_type)
            if callable(generate_heatmap_matrix)
            else {"heatmap_data": {}, "totals": {}, "df_with_zone": df}
        )

        # ✅ IMPORTANT: Use df_with_zone (contains zone_key per ball)
        df_zoned = hm.get("df_with_zone")
        if df_zoned is None or df_zoned.empty:
            df_zoned = df.copy()

        # ---------------- Prepare Pitch Points ----------------
        keep_cols = [
            "scrM_PitchXPos", "scrM_PitchYPos", "scrM_PitchX", "scrM_PitchY",
            "scrM_BatsmanRuns", "scrM_IsBoundry", "scrM_IsSixer", "scrM_IsWicket",
            "scrM_VideoFile", "scrM_Video1URL", "scrM_DelId", "scrM_MatchName",
            "scrM_OverNo", "scrM_DelNo", "scrM_InningNo", "scrM_BallID",
            "scrM_StrikerBatterSkill", "scrM_BowlerSkill",
            "scrM_PlayMIdStrikerName", "scrM_PlayMIdBowlerName",
            "zone_key", "Zone"
        ]

        pitch_points = []
        isna = pd.isna

        for _, row in df_zoned.iterrows():
            point = {}

            # ✅ copy columns safely
            for c in keep_cols:
                if c in df_zoned.columns:
                    val = row.get(c, None)
                    try:
                        if hasattr(val, "item"):
                            val = val.item()
                    except Exception:
                        pass
                    if val is not None and not isna(val):
                        point[c] = val

            # ✅ Determine ball ID
            ball_id = None
            for cand in ("scrM_BallID", "scrM_BallId", "scrM_DelId", "scrM_DelID", "id", "ball_id"):
                if cand in df_zoned.columns:
                    val = row.get(cand)
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        continue
                    s = str(val).strip()
                    if s:
                        ball_id = s
                        break

            if not ball_id:
                continue

            # ✅ standardize ball id key
            point["scrM_BallID"] = ball_id

            # ✅ Ensure python heatmap click mapping exists
            if "zone_key" not in point or not point.get("zone_key"):
                z = row.get("Zone")
                if z is not None and not (isinstance(z, float) and pd.isna(z)):
                    point["zone_key"] = str(z).strip()

            pitch_points.append(point)

        # ---------------- Return JSON ----------------
        return jsonify({
            "heatmap_data": hm.get("heatmap_data", {}) if isinstance(hm, dict) else {},
            "totals": hm.get("totals", {}) if isinstance(hm, dict) else {},
            "pitch_points": pitch_points
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500









# ✅ AJAX endpoint for updating Line & Length metric dynamically
# @apps.route("/apps/update_metric", methods=["POST"])
# @login_required
# def update_metric():
#     try:
#         from flask import jsonify

#         metric = request.json.get("metric")
#         selected_type = request.json.get("type") or "batter"
#         selected_team = request.json.get("team")
#         selected_matches = request.json.get("matches", [])
#         selected_tournament = request.json.get("tournament")

#         # ✅ Fetch latest filtered ball-by-ball data (reuse logic)
#         ball_by_ball_df = get_ball_by_ball_details(selected_matches)


#         if ball_by_ball_df.empty:
#             return jsonify({"error": "No data"}), 200

#         # ✅ Filter by team
#         if selected_team:
#             if selected_type == "batter":
#                 ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBattingName'] == selected_team]
#             else:
#                 ball_by_ball_df = ball_by_ball_df[ball_by_ball_df['scrM_tmMIdBowlingName'] == selected_team]

#         # ✅ Generate updated metric report
#         report = generate_line_length_report(ball_by_ball_df, metric)

#         return jsonify({"heatmap_data": report.get("heatmap_data", {})})

#     except Exception as e:
#         print("❌ AJAX metric update failed:", e)
#         return jsonify({"error": str(e)}), 500
    
# @apps.route("/apps/update_heatmap", methods=["POST"])
# @login_required
# def update_heatmap():
#     """
#     AJAX endpoint – rebuilds the line & length heatmap using
#     BOTH the advanced-filter context and the quick UI filters.
#     """
#     try:
#         data = request.get_json(force=True) or {}
#         metric        = (data.get("metric") or "").strip()
#         batter_skill  = (data.get("batter_skill") or "all").upper()
#         bowler_skill  = (data.get("bowler_skill") or "all").upper()
#         team          = data.get("team")
#         tournament    = data.get("tournament")
#         matches       = data.get("matches", [])
#         view_type     = data.get("type") or "batter"

#         # 👇 advanced-filter additions
#         day           = data.get("day")
#         inning        = data.get("inning")
#         session       = data.get("session")
#         phase         = data.get("phase")
#         from_over     = data.get("from_over")
#         to_over       = data.get("to_over")
#         ball_phase    = data.get("ball_phase")

#         print(f"🟢 AJAX Heatmap → metric={metric}, batter={batter_skill}, bowler={bowler_skill}")

#         if not matches:
#             return jsonify({"heatmap_data": {}})

#         conn = get_connection()
#         df = get_filtered_score_data(
#             conn,
#             matches,
#             day=day,
#             inning=inning,
#             session=session,
#             phase=phase,
#             from_over=from_over,
#             to_over=to_over,
#             type=view_type,
#             batters=[],   # we’ll refine below
#             bowlers=[]
#         )
#         conn.close()

#         if df.empty:
#             print("⚠️ empty df after base filter")
#             return jsonify({"heatmap_data": {}})

#         # ✅ team filter
#         if team:
#             if view_type == "batter":
#                 df = df[df["scrM_tmMIdBattingName"] == team]
#             else:
#                 df = df[df["scrM_tmMIdBowlingName"] == team]

#         # ✅ apply quick-filter skills
#         if batter_skill != "ALL" and "scrM_StrikerBatterSkill" in df.columns:
#             df = df[df["scrM_StrikerBatterSkill"].str.upper().str.contains(batter_skill, na=False)]
#         if bowler_skill != "ALL" and "scrM_BowlerSkill" in df.columns:
#             df = df[df["scrM_BowlerSkill"].str.upper().str.contains(bowler_skill, na=False)]

#         if df.empty:
#             print("⚠️ empty df after skill filter")
#             return jsonify({"heatmap_data": {}})

#         # ✅ line & length report (handles metric)
#         report = generate_line_length_report(df, metric)

#         return jsonify({"heatmap_data": report.get("heatmap_data", {})})

#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500
    
# @apps.route('/apps/heatmap_videos', methods=['POST'])
# @login_required
# def heatmap_videos():
#     print("🔥 /apps/heatmap_videos called")
#     """
#     AJAX endpoint called when a heatmap box is clicked.
#     Expects JSON body with:
#       - matches: [] (list) OR 'selected_matches' from form
#       - line: numeric or string
#       - length: numeric or string
#       - metric: string (optional)
#       - extra filters: batters[], bowlers[], inning, session, day, from_over, to_over
#     Returns: JSON { videos: [ {del_id, match, over, ball, video_files:[...]} ] }
#     """
#     try:
#         payload = request.get_json(force=True)
#         matches = payload.get("matches") or payload.get("selected_matches") or []
#         line = payload.get("line")
#         length = payload.get("length")
#         metric = payload.get("metric")
#         batters = payload.get("batters")
#         bowlers = payload.get("bowlers")
#         inning = payload.get("inning")
#         session = payload.get("session")
#         day = payload.get("day")
#         from_over = payload.get("from_over")
#         to_over = payload.get("to_over")

#         # Defensive conversion: ensure matches is list
#         if isinstance(matches, str):
#             matches = [matches]

#         videos = fetch_metric_videos(
#             match_names=matches,
#             line=line,
#             length=length,
#             metric=metric,
#             batters=batters,
#             bowlers=bowlers,
#             inning=inning,
#             session=session,
#             day=day,
#             from_over=from_over,
#             to_over=to_over
#         )

#         # Flatten or keep as-is depending on client need
#         return jsonify({"ok": True, "videos": videos})
#     except Exception as e:
#         print("❌ heatmap_videos endpoint error:", e)
#         return jsonify({"ok": False, "error": str(e)}), 500









from flask import Flask, request, render_template_string, jsonify
import pyodbc

app = Flask(__name__)

# ---------- Serve local video files safely ----------
@apps.route("/local_video")
def local_video():
    path = request.args.get("path")
    if not path:
        abort(404)
    videos_root = os.path.realpath(os.path.join(os.getcwd(), "static", "videos"))
    full = os.path.realpath(path)
    if not full.startswith(videos_root):
        print("[local_video] ❌ blocked outside folder:", full)
        abort(403)
    if not os.path.exists(full):
        abort(404)
    return send_file(full)

# -------------------- VIDEO PLAYER ROUTE --------------------
@apps.route("/video_player", methods=["GET", "POST"])
def video_player():
    import urllib.parse, os, pandas as pd
    from flask import request, render_template_string, url_for
    from tailwick.utils import get_connection

    video_list = []

    # -----------------------------
    # 1️⃣ Handle POST form submission (secure version)
    # -----------------------------
    if request.method == "POST":
        videos_post = request.form.get("videos", "")
        if videos_post:
            for p in videos_post.split(","):
                if p.strip():
                    video_list.append(p.strip())

    # -----------------------------
    # 2️⃣ Handle GET params
    # -----------------------------
    balls_param = request.args.get("balls", "")
    videos_param = request.args.get("videos", "")

    # If ?videos= is provided directly
    if not video_list and videos_param:
        for path in videos_param.split(","):
            path = path.strip()
            if path:
                video_list.append(path)

    # 🆕 Handle ?balls=1234,1235,...  (Heatmap Zone Playback)
    if balls_param and not video_list:
        try:
            ball_ids = [b.strip() for b in balls_param.split(",") if b.strip()]
            print(f"🎯 /video_player received {len(ball_ids)} ball IDs for zone playback")

            conn = get_connection()
            query = f"""
                SELECT scrM_DelId,
                       scrM_Video1URL, scrM_Video2URL, scrM_Video3URL,
                       scrM_Video4URL, scrM_Video5URL, scrM_Video6URL,
                       scrM_Video1FileName, scrM_Video2FileName, scrM_Video3FileName,
                       scrM_Video4FileName, scrM_Video5FileName, scrM_Video6FileName
                FROM tblscoremaster
                WHERE scrM_DelId IN ({','.join(['%s'] * len(ball_ids))})
            """
            df = pd.read_sql(query, conn, params=tuple(ball_ids))
            conn.close()

            for _, row in df.iterrows():
                for i in range(1, 7):
                    v_url = row.get(f"scrM_Video{i}URL")
                    v_file = row.get(f"scrM_Video{i}FileName")

                    if v_url and isinstance(v_url, str) and v_url.strip():
                        video_list.append(v_url.strip())
                    elif v_file and isinstance(v_file, str) and v_file.strip():
                        local_path = os.path.join("static", "videos", v_file.strip())
                        if os.path.exists(local_path):
                            video_list.append(local_path)
                        else:
                            video_list.append(os.path.abspath(local_path))

            video_list = list(dict.fromkeys(video_list))  # dedupe
            print(f"✅ Found {len(video_list)} videos for heatmap balls")
        except Exception as e:
            print(f"❌ Error fetching videos for balls param: {e}")
            video_list = []

    # -----------------------------
    # 3️⃣ Handle delivery-id based video lookup
    # -----------------------------
    batter_id = request.args.get("batter_id")
    bowler_id = request.args.get("bowler_id")
    metric_raw = request.args.get("metric")
    match_id = request.args.get("match_id")
    inning_id = request.args.get("inning_id")

    # ✅ FIX: keep match_id numeric if possible
    try:
        if match_id is not None:
            match_id = int(match_id)
    except:
        pass

    try:
        if inning_id is not None:
            inning_id = int(inning_id)
    except:
        pass

    del_id = request.args.get("del_id") or request.args.get("delid") or request.args.get("delivery_id")

    # ✅ Single ball playback
    if del_id and not video_list:
        try:
            print(f"🎯 /video_player received single-ball del_id={del_id}")

            conn = get_connection()
            query = """
                SELECT scrM_DelId,
                       scrM_Video1URL, scrM_Video2URL, scrM_Video3URL,
                       scrM_Video4URL, scrM_Video5URL, scrM_Video6URL,
                       scrM_Video1FileName, scrM_Video2FileName, scrM_Video3FileName,
                       scrM_Video4FileName, scrM_Video5FileName, scrM_Video6FileName
                FROM tblscoremaster
                WHERE scrM_DelId = %s
                LIMIT 1
            """
            df = pd.read_sql(query, conn, params=(del_id,))
            conn.close()

            if not df.empty:
                row = df.iloc[0].to_dict()
                for i in range(1, 7):
                    v_url = row.get(f"scrM_Video{i}URL")
                    v_file = row.get(f"scrM_Video{i}FileName")

                    if v_url and isinstance(v_url, str) and v_url.strip():
                        video_list.append(v_url.strip())
                    elif v_file and isinstance(v_file, str) and v_file.strip():
                        local_path = os.path.join("static", "videos", v_file.strip())
                        if os.path.exists(local_path):
                            video_list.append(local_path)
                        else:
                            video_list.append(os.path.abspath(local_path))

            video_list = list(dict.fromkeys(video_list))
            print(f"✅ Found {len(video_list)} videos for delivery {del_id}")

        except Exception as e:
            print(f"❌ Error fetching video for del_id={del_id}: {e}")
            video_list = []

    # -----------------------------
    # 4️⃣ Normalize metric (✅ FIXED)
    # -----------------------------
    # ✅ IMPORTANT:
    # Keep metrics as full names because fetch_metric_videos() expects those
    metric_map = {
        "runs": "runs",
        "balls": "balls",
        "fours": "fours",
        "sixes": "sixes",
        "dots": "dots",
        "wickets": "wickets",

        # If bowler metrics are coming as short
        "O": "overs",
        "R": "runs",
        "W": "wickets",
        "D": "dots",
        "M": "maidens",

        # If someone sends full already
        "overs": "overs",
        "maidens": "maidens",
    }

    metric_raw = (metric_raw or "").strip()
    metric = metric_map.get(metric_raw, metric_raw)

    # -----------------------------
    # 5️⃣ Multi-ball fallback using fetch_metric_videos()
    # -----------------------------
    if not video_list and (batter_id or bowler_id) and metric and match_id and inning_id:
        try:
            print("🎥 fetch_metric_videos called with =>",
                  f"batter_id={batter_id}, bowler_id={bowler_id}, metric={metric}, match_id={match_id}, inning_id={inning_id}")

            video_list = fetch_metric_videos(
                batter_id=batter_id,
                bowler_id=bowler_id,
                metric=metric,   # ✅ sends full word metric now
                match_id=match_id,
                inning_id=inning_id
            )

        except Exception as e:
            print("❌ fetch_metric_videos failed:", e)
            video_list = []

    # -----------------------------
    # 6️⃣ Convert local paths to safe URLs
    # -----------------------------
    static_root = os.path.realpath(os.path.join(os.getcwd(), "static"))

    def make_accessible_url(v):
        if not v:
            return None
        v = str(v)

        if v.startswith("http://") or v.startswith("https://"):
            return v

        if v.startswith("/"):
            return v

        abs_v = os.path.realpath(v)

        if os.path.exists(abs_v):
            if abs_v.startswith(static_root):
                rel = os.path.relpath(abs_v, static_root).replace(os.path.sep, "/")
                return url_for("static", filename=rel)
            return url_for("local_video") + "?path=" + urllib.parse.quote_plus(abs_v)

        return v

    final_videos = []
    seen = set()

    for v in video_list:
        url = make_accessible_url(v)
        if url and url not in seen:
            seen.add(url)
            final_videos.append(url)

    print(f"✅ /video_player returning {len(final_videos)} videos")

    # -----------------------------
    # 7️⃣ Render video player UI
    # -----------------------------
    template = """
    <!DOCTYPE html>
    <html>
    <head>
      <title>Video Player with Drawing</title>
      <style>
        html, body { margin: 0; padding: 0; width: 100%; height: 100%; background: #000;
          font-family: sans-serif; color: white; display: flex; justify-content: center; align-items: center; }
        .container { width: 98%; max-width: 900px; aspect-ratio: 16 / 9; position: relative;
          background: #000; border-radius: 6px; overflow: hidden; box-shadow: 0 10px 20px rgba(0,0,0,0.3); }
        .container.fullscreen { width: 100%; max-width: 100%; height: 100vh; aspect-ratio: auto; border-radius: 0; }
        video, canvas { width: 100%; height: 100%; object-fit: contain; display: block; }
        canvas { position: absolute; top: 0; left: 0; pointer-events: auto; }
        #toolbar { position: absolute; bottom: 60px; left: 50%; transform: translateX(-50%);
          background: rgba(0,0,0,0.7); padding: 8px 12px; border-radius: 10px; display: flex; gap: 8px;
          align-items: center; z-index: 10; }
        .tool-btn { background: none; border: none; color: white; cursor: pointer; font-size: 18px; }
        .tool-btn:hover { color: #0ff; }
        input[type="color"] { width: 24px; height: 24px; border: none; border-radius: 50%; cursor: pointer; }
        #timeControls { position: absolute; bottom: 0; left: 0; width: 100%;
          background: rgba(0,0,0,0.7); padding: 6px; display: flex; flex-direction: column; align-items: center;
          opacity: 0; transition: opacity 0.3s; }
        .container:hover #timeControls { opacity: 1; }
        .progress-container { width: 95%; display: flex; align-items: center; gap: 8px; }
        input[type=range] { flex: 1; }
        .time-buttons { margin-top: 4px; display: flex; gap: 10px; }
        .time-buttons button { background: rgba(255,255,255,0.1); border: none; color: white;
          padding: 4px 10px; border-radius: 6px; cursor: pointer; }
        .time-buttons button:hover { background: rgba(255,255,255,0.3); }
        .status { margin-top: 3px; font-size: 13px; color: #aaa; }
      </style>
    </head>
    <body>
      <div class="container" id="videoContainer">
        <video id="player" autoplay playsinline></video>
        <canvas id="drawCanvas"></canvas>

        <div id="toolbar">
          <button class="tool-btn" onclick="setTool('pen')">✏️</button>
          <button class="tool-btn" onclick="setTool('eraser')">🩹</button>
          <button class="tool-btn" onclick="setTool('rect')">⬛</button>
          <button class="tool-btn" onclick="setTool('square')">▢</button>
          <button class="tool-btn" onclick="setTool('circle')">⚪</button>
          <button class="tool-btn" onclick="setTool('triangle')">🔺</button>
          <button class="tool-btn" onclick="undo()">↩️</button>
          <button class="tool-btn" onclick="clearCanvas()">🗑</button>
          <input type="color" id="colorPicker" value="#ff0000" onchange="setColor(this.value)">
          <button class="tool-btn" onclick="toggleFullscreen()">⛶</button>
        </div>

        <div id="timeControls">
          <div class="progress-container">
            <span id="currentTime">0:00</span>
            <input type="range" id="seekBar" value="0" min="0" step="0.1">
            <span id="duration">0:00</span>
          </div>
          <div class="time-buttons">
            <button onclick="prevVideo()">⏮ Prev</button>
            <button onclick="rewind()">⏪ 10s</button>
            <button onclick="togglePlayPause()" id="playPause">⏸ Pause</button>
            <button onclick="forward()">10s ⏩</button>
            <button onclick="nextVideo()">Next ⏭</button>
          </div>
          <div class="status" id="videoStatus"></div>
        </div>
      </div>

      <script>
        let videos = {{ videos|tojson }};
        let index = 0;
        let player = document.getElementById("player");
        const canvas = document.getElementById("drawCanvas");
        const ctx = canvas.getContext("2d");
        const container = document.getElementById("videoContainer");
        const seekBar = document.getElementById("seekBar");
        const currentTimeLabel = document.getElementById("currentTime");
        const durationLabel = document.getElementById("duration");
        const playPauseBtn = document.getElementById("playPause");
        const videoStatus = document.getElementById("videoStatus");

        function formatTime(sec){
          let m=Math.floor(sec/60), s=Math.floor(sec%60);
          return m+":"+(s<10?"0"+s:s);
        }

        function loadVideo(i){
          if(i<0||i>=videos.length) return;
          index=i;
          player.src=videos[index];
          player.play();
          resizeCanvas();
          videoStatus.textContent="Playing "+(index+1)+" of "+videos.length;
        }

        function nextVideo(){ loadVideo(index+1); }
        function prevVideo(){ loadVideo(index-1); }

        if(videos.length>0){
          loadVideo(0);
        } else {
          alert("No video found for this metric");
        }

        let drawing=false,startX=0,startY=0,tool="pen",color="red",lineWidth=3,history=[];

        function resizeCanvas(){
          canvas.width=player.clientWidth;
          canvas.height=player.clientHeight;
        }

        window.addEventListener("resize",resizeCanvas);

        player.addEventListener("loadedmetadata",()=>{
          resizeCanvas();
          seekBar.max=player.duration;
          durationLabel.textContent=formatTime(player.duration);
        });

        document.addEventListener("fullscreenchange",()=>{
          container.classList.toggle("fullscreen",!!document.fullscreenElement);
          resizeCanvas();
        });

        function setTool(t){tool=t;}
        function setColor(c){color=c;}

        function saveState(){history.push(canvas.toDataURL());}

        function undo(){
          if(history.length>0){
            let img=new Image();
            img.onload=()=>{
              ctx.clearRect(0,0,canvas.width,canvas.height);
              ctx.drawImage(img,0,0);
            };
            img.src=history.pop();
          }
        }

        function clearCanvas(){
          ctx.clearRect(0,0,canvas.width,canvas.height);
          history=[];
        }

        canvas.addEventListener("mousedown",e=>{
          saveState();
          drawing=true;
          startX=e.offsetX;
          startY=e.offsetY;
          if(tool==='pen'||tool==='eraser'){
            ctx.beginPath();
            ctx.moveTo(startX,startY);
          }
        });

        canvas.addEventListener("mousemove",e=>{
          if(!drawing) return;
          if(tool==='pen'){
            ctx.globalCompositeOperation='source-over';
            ctx.strokeStyle=color;
            ctx.lineWidth=lineWidth;
            ctx.lineTo(e.offsetX,e.offsetY);
            ctx.stroke();
          } else if(tool==='eraser'){
            ctx.globalCompositeOperation='destination-out';
            ctx.lineWidth=10;
            ctx.lineTo(e.offsetX,e.offsetY);
            ctx.stroke();
          }
        });

        canvas.addEventListener("mouseup",e=>{
          if(!drawing) return;
          drawing=false;
          ctx.globalCompositeOperation='source-over';
          let endX=e.offsetX,endY=e.offsetY;
          ctx.strokeStyle=color;
          ctx.lineWidth=lineWidth;

          if(tool==='rect'){
            ctx.strokeRect(startX,startY,endX-startX,endY-startY);
          } else if(tool==='square'){
            let size=Math.min(Math.abs(endX-startX),Math.abs(endY-startY));
            ctx.strokeRect(startX,startY,size,size);
          } else if(tool==='circle'){
            let radius=Math.sqrt(Math.pow(endX-startX,2)+Math.pow(endY-startY,2));
            ctx.beginPath();
            ctx.arc(startX,startY,radius,0,Math.PI*2);
            ctx.stroke();
          } else if(tool==='triangle'){
            ctx.beginPath();
            ctx.moveTo(startX,startY);
            ctx.lineTo(endX,endY);
            ctx.lineTo(startX*2-endX,endY);
            ctx.closePath();
            ctx.stroke();
          }
        });

        player.addEventListener("timeupdate",()=>{
          seekBar.value=player.currentTime;
          currentTimeLabel.textContent=formatTime(player.currentTime);
        });

        seekBar.addEventListener("input",()=>{
          player.currentTime=seekBar.value;
        });

        function togglePlayPause(){
          if(player.paused){
            player.play();
            playPauseBtn.textContent="⏸ Pause";
          } else {
            player.pause();
            playPauseBtn.textContent="▶️ Play";
          }
        }

        function rewind(){ player.currentTime=Math.max(0,player.currentTime-10); }
        function forward(){ player.currentTime=Math.min(player.duration,player.currentTime+10); }

        function toggleFullscreen(){
          if(!document.fullscreenElement){
            container.requestFullscreen();
          } else {
            document.exitFullscreen();
          }
        }

        // ✅ AUTO-PLAY NEXT VIDEO WITH LOOP
        player.addEventListener("ended", ()=>{
          if(index < videos.length - 1){
            nextVideo();
          } else {
            videoStatus.textContent = "Restarting playlist (1 of " + videos.length + ")";
            loadVideo(0);
          }
        });
      </script>
    </body>
    </html>
    """
    return render_template_string(template, videos=final_videos)



    


# from flask import send_file, abort, request
# import io
# from playwright.sync_api import sync_playwright
# from reportlab.lib.pagesizes import A4
# from reportlab.pdfgen import canvas
# from reportlab.lib.utils import ImageReader
# from PIL import Image

# def build_player_card_html(player_df, player, selected_type):
#     # ✅ Generate strengths & weaknesses dynamically
#     strengths_html, weaknesses_html = generate_dynamic_strengths_weaknesses(player_df, player, selected_type)

#     # ✅ Build the KPI card (you must already have this)
#     kpi_card_html = generate_kpi_card(player_df, player, selected_type)

#     # ✅ Insert everything into one combined HTML
#     combined_html = f"""
#         <div class="mb-6" style="max-width:100%;overflow:hidden; position:relative;">
#             <!-- BLUE CARD -->
#             <div class="card bg-sky-500 border-sky-500 dark:bg-sky-800 dark:border-sky-800"
#                 style="border-radius:10px;padding:12px;display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;">
#                 <div style="flex:1;min-width:260px;max-width:100%;">{kpi_card_html}</div>
#             </div>

#             <!-- Strengths & Weaknesses -->
#             <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:20px;">
#                 <div class="card border border-green-400 bg-green-50 dark:bg-green-900 dark:border-green-700"
#                     style="flex:1;min-width:280px;border-radius:10px;padding:12px;">
#                     <div class="card-body">
#                         <h6 class="mb-3 text-15 font-semibold text-green-700 dark:text-green-300">Strengths</h6>
#                         {strengths_html}
#                     </div>
#                 </div>
#                 <div class="card border border-red-400 bg-red-50 dark:bg-red-900 dark:border-red-700"
#                     style="flex:1;min-width:280px;border-radius:10px;padding:12px;">
#                     <div class="card-body">
#                         <h6 class="mb-3 text-15 font-semibold text-red-700 dark:text-red-300">Weaknesses</h6>
#                         {weaknesses_html}
#                     </div>
#                 </div>
#             </div>
#         </div>
#     """
#     return combined_html


from flask import abort, request, jsonify
import io, os, sys, webbrowser
from pathlib import Path
from playwright.sync_api import sync_playwright
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from PIL import Image

def get_downloads_folder():
    """Return system Downloads folder (cross-platform)."""
    return str(Path.home() / "Downloads")

@apps.route("/download_pdf/<string:player_name>")
def download_pdf(player_name):
    global player_kpi_cache

    combined_html = player_kpi_cache.get(player_name)
    if not combined_html:
        abort(404, "No data found for this player")

    # ✅ Custom dark mode CSS with chart text overrides
    inline_css = """
    <style>
      body {
        background:#132337; /* outer background card */
        margin:0;
        padding:24px;
        font-family:ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, Noto Sans;
        color:#cbd5e1;
      }
      .outer-card {
        background:#132337; /* requested outer card color */
        border-radius:12px;
        padding:20px;
        box-shadow:0 2px 6px rgba(0,0,0,.5);
      }
      .bg-sky-500 {
        background:#075985 !important; /* requested player profile card color */
        color:#fff;
      }
      table {
        width:100%;
        border-collapse: collapse;
        margin-top:16px;
        font-size:14px;
        border-radius:8px;
        overflow:hidden;
      }
      th, td {
        padding:10px 12px;
        text-align:center;
        border:1px solid #1B355B;
      }
      th {
        background:#1B355B;
        color:#fff;
        font-weight:600;
        font-size:14px;
      }
      tr td {
        background:#1E3D6C;
        color:#f1f5f9;
      }
      tr:nth-child(even) td {
        background:#24497A;
      }
      tr:hover td {
        background:#2563eb;
        color:#fff;
      }
      h2, h3, h4 {
        color:#f1f5f9;
      }

      /* ✅ PDF look fixes */
      /* Productive Areas (Top 3) axis labels */
      .apexcharts-xaxis text,
      .apexcharts-yaxis text {
        fill: #808080 !important;
      }

      /* Productive Shots (Top 3) above-bar labels */
      .apexcharts-datalabel {
        fill: #808080 !important;
        color: #808080 !important;
      }

      /* Wickets Breakdown legend */
      .js-plotly-plot .legend text {
        fill: #808080 !important;
        color: #808080 !important;
      }
    </style>
    """

    # ✅ Wrap everything in the outer card
    html_content = f"""
    <!DOCTYPE html><html lang="en">
    <head>
      <meta charset="UTF-8"><title>{player_name} Report</title>
      {inline_css}
      <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
      <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
      <div class="outer-card" id="player-card">
        {combined_html}
      </div>
    </body>
    </html>
    """

    downloads_folder = get_downloads_folder()
    pdf_path = os.path.join(downloads_folder, f"{player_name}_report.pdf")

    try:
        # ✅ Ensure Playwright uses bundled Chromium in EXE
        with sync_playwright() as p:
            if getattr(sys, 'frozen', False):  # Running in PyInstaller EXE mode
                chromium_path = os.path.join(
                    sys._MEIPASS, "ms-playwright", "chromium-1181", "chrome-win", "chrome.exe"
                )
                print(f"Using bundled Chromium at: {chromium_path}")
                browser = p.chromium.launch(executable_path=chromium_path)
            else:
                # Normal Python mode
                browser = p.chromium.launch()

            # --- Render the HTML to screenshot ---
            page = browser.new_page()
            page.set_content(html_content)

            # Wait for dynamic content
            page.wait_for_selector("#player-card", state="visible", timeout=10000)
            try:
                page.wait_for_selector(".apexcharts-canvas", timeout=5000)
            except:
                pass
            try:
                page.wait_for_selector(".js-plotly-plot", timeout=5000)
            except:
                pass

            # Small delay to ensure charts fully render
            page.wait_for_timeout(1000)

            # Capture full-page screenshot
            screenshot_bytes = page.screenshot(type="png", full_page=True)
            browser.close()

        # --- Convert PNG → PDF (A4 page size) ---
        image = Image.open(io.BytesIO(screenshot_bytes))
        img_width, img_height = image.size
        c = canvas.Canvas(pdf_path, pagesize=A4)
        page_w, page_h = A4
        scale = min(page_w / img_width, page_h / img_height)
        new_w, new_h = img_width * scale, img_height * scale
        x, y = (page_w - new_w) / 2, (page_h - new_h) / 2
        c.drawImage(ImageReader(image), x, y, new_w, new_h)
        c.showPage()
        c.save()

    except Exception as e:
        print("⚠️ Playwright failed, using ReportLab fallback:", e)

        # --- Fallback: Generate simple text PDF using ReportLab ---
        doc = SimpleDocTemplate(pdf_path, pagesize=A4)
        styles = getSampleStyleSheet()
        # Note: ReportLab does NOT support raw HTML fully
        story = [
            Paragraph(f"{player_name} Report", styles["Heading1"]),
            Paragraph(combined_html, styles["Normal"])
        ]
        doc.build(story)

    # ✅ Send the generated PDF file for download
    return send_file(
        pdf_path,
        as_attachment=True,
        download_name=f"{player_name}_report.pdf",
        mimetype="application/pdf"
    )


# at the top of apps.py
BATTER_DATA = {}   # cache batter_id -> {"df": DataFrame, "hand": "Right/Left"}

match_report_cache = {}

# @apps.route('/apps/apps-mailbox', methods=['GET', 'POST'])
# def mailbox_filters():
#     tournaments = get_all_tournaments()

#     # Form selections
#     selected_tournament = request.form.get("tournament") if request.method == "POST" else None
#     selected_team = request.form.get("team") if request.method == "POST" else None
#     selected_match = request.form.get("matches") if request.method == "POST" else None

#     # Dependent dropdowns
#     teams = get_teams_by_tournament(selected_tournament) if selected_tournament else []
#     matches = get_matches_by_team(selected_team, selected_tournament) if selected_team else []


#     # Default objects
#     match_header, match_innings, last_12_balls = None, [], []
#     scorecard_data, inning_summary, fall_of_wickets, bowler_scorecard = {}, {}, {}, {}
#     partnership_charts = {}
#     multi_day_report, limited_overs_report = None, None
#     runs_per_over_chart, run_rate_chart = None, None

#     # ✅ initialize charts to None so they exist
#     donut_chart = None
#     extra_runs_chart = None
#     area_wise_chart = None
#     phase_reports = {}

#     # Detect match format
#     match_format_code = None
#     if selected_tournament:
#         try:
#             match_format_code = get_match_format_code_by_tournament(selected_tournament)
#         except Exception as e:
#             print("⚠️ Could not fetch match format:", e)

#     # If match is selected → fetch details
#     if selected_match:
#         match_header = get_match_header(selected_match)
#         match_innings = get_match_innings(selected_match)
#         last_12_balls = get_last_12_deliveries(selected_match)

#         ball_by_ball_df = get_ball_by_ball_data(selected_match)
#         if not ball_by_ball_df.empty:
#             # --- Batting scorecard ---
#             for inn, inn_df in ball_by_ball_df.groupby('scrM_InningNo'):
#                 batters = []
#                 for idx, (batter, bdf) in enumerate(inn_df.groupby('scrM_PlayMIdStrikerName')):
#                     runs = bdf['scrM_BatsmanRuns'].sum()
#                     balls = len(bdf)
#                     dots = (bdf['scrM_BatsmanRuns'] == 0).sum()
#                     fours = (bdf['scrM_BatsmanRuns'] == 4).sum()
#                     sixes = (bdf['scrM_BatsmanRuns'] == 6).sum()
#                     ones = (bdf['scrM_BatsmanRuns'] == 1).sum()
#                     twos = (bdf['scrM_BatsmanRuns'] == 2).sum()
#                     threes = (bdf['scrM_BatsmanRuns'] == 3).sum()

#                     sr = round((runs / balls) * 100, 2) if balls else 0
#                     bdry_pct = round(((fours + sixes) / balls) * 100, 2) if balls else 0
#                     bdry_freq = round(balls / (fours + sixes), 2) if (fours + sixes) > 0 else 0
#                     db_pct = round((dots / balls) * 100, 2) if balls else 0
#                     db_freq = round(balls / dots, 2) if dots > 0 else 0

#                     dismissal = "not out"
#                     wk = bdf[bdf['scrM_IsWicket'] == 1]
#                     if not wk.empty:
#                         d = wk.iloc[0]
#                         caught = d.get('scrM_playMIdCaughtName') or ''
#                         dismissal = f"c {caught} b {d['scrM_PlayMIdBowlerName']}"

#                     bowler_table = []
#                     for bowler, bowdf in bdf.groupby('scrM_PlayMIdBowlerName'):
#                         bowler_table.append({
#                             "bowler": bowler,
#                             "runs": bowdf['scrM_BatsmanRuns'].sum(),
#                             "balls": len(bowdf),
#                             "dots": (bowdf['scrM_BatsmanRuns'] == 0).sum(),
#                             "ones": (bowdf['scrM_BatsmanRuns'] == 1).sum(),
#                             "twos": (bowdf['scrM_BatsmanRuns'] == 2).sum(),
#                             "threes": (bowdf['scrM_BatsmanRuns'] == 3).sum(),
#                             "fours": (bowdf['scrM_BatsmanRuns'] == 4).sum(),
#                             "sixes": (bowdf['scrM_BatsmanRuns'] == 6).sum(),
#                         })

#                     hand_val = bdf["BatterHand"].dropna().unique()
#                     batter_hand = hand_val[0] if len(hand_val) > 0 else None

#                     if "scrM_BattingPosition" in bdf.columns and not bdf["scrM_BattingPosition"].isna().all():
#                         batting_pos = int(bdf["scrM_BattingPosition"].iloc[0])
#                     else:
#                         bdf["ball_index"] = (bdf["scrM_OverNo"] - 1) * 6 + bdf["scrM_DelNo"]
#                         batting_pos = int(bdf["ball_index"].min())

#                     batter_id = f"{selected_match}_{inn}_{batting_pos}"
#                     BATTER_DATA[batter_id] = {"df": bdf, "hand": batter_hand}

#                     real_playmid = (
#                         int(bdf["scrM_PlayMIdStriker"].iloc[0])
#                         if "scrM_PlayMIdStriker" in bdf.columns and not bdf["scrM_PlayMIdStriker"].isna().all()
#                         else None
#                     )

#                     wagon_wheel_img = generate_wagon_wheel(bdf, batter_hand)

#                     batters.append({
#                         "id": batter_id,
#                         "PlayMId": real_playmid,
#                         "MatchName": selected_match,
#                         "InningNo": inn,
#                         "position": batting_pos,
#                         "name": batter,
#                         "runs": runs,
#                         "balls": balls,
#                         "dots": dots,
#                         "ones": ones,
#                         "twos": twos,
#                         "threes": threes,
#                         "fours": fours,
#                         "sixes": sixes,
#                         "sr": sr,
#                         "bdry_pct": bdry_pct,
#                         "bdry_freq": bdry_freq,
#                         "db_pct": db_pct,
#                         "db_freq": db_freq,
#                         "dismissal": dismissal,
#                         "bowler_table": bowler_table,
#                         "hand": batter_hand,
#                         "wagon_wheel": wagon_wheel_img
#                     })
#                 scorecard_data[inn] = sorted(batters, key=lambda x: x["position"])


#             # --- Inning Summary + Fall of Wickets + Bowler Scorecard ---
#             for inn, inn_df in ball_by_ball_df.groupby("scrM_InningNo"):
#                 total_runs = inn_df['scrM_DelRuns'].sum() if 'scrM_DelRuns' in inn_df.columns else 0

#                 mask = pd.Series(True, index=inn_df.index)
#                 if 'scrM_IsWideBall' in inn_df.columns:
#                     mask &= (inn_df['scrM_IsWideBall'] == 0)
#                 if 'scrM_IsNoBall' in inn_df.columns:
#                     mask &= (inn_df['scrM_IsNoBall'] == 0)

#                 legal_balls = inn_df[mask]
#                 balls = len(legal_balls)
#                 overs = f"{balls // 6}.{balls % 6}"
#                 crr = round(total_runs / (balls / 6), 2) if balls else 0

#                 no_balls = inn_df['scrM_IsNoBall'].sum() if 'scrM_IsNoBall' in inn_df.columns else 0
#                 wide_balls = inn_df['scrM_IsWideBall'].sum() if 'scrM_IsWideBall' in inn_df.columns else 0
#                 byes = inn_df['scrM_ByeRuns'].sum() if 'scrM_ByeRuns' in inn_df.columns else 0
#                 leg_byes = inn_df['scrM_LegByeRuns'].sum() if 'scrM_LegByeRuns' in inn_df.columns else 0
#                 penalty = inn_df['scrM_PenaltyRuns'].sum() if 'scrM_PenaltyRuns' in inn_df.columns else 0

#                 extras_total = no_balls + wide_balls + byes + leg_byes + penalty
#                 wickets = inn_df['scrM_IsWicket'].sum() if 'scrM_IsWicket' in inn_df.columns else 0

#                 # ✅ Partnership calculation
#                 partnerships = []
#                 if not inn_df.empty:
#                     inn_df["Partnership_Key"] = inn_df.apply(
#                         lambda row: "_&_".join(sorted([
#                             str(row.get("scrM_PlayMIdStrikerName", "")),
#                             str(row.get("scrM_PlayMIdNonStrikerName", ""))
#                         ])),
#                         axis=1
#                     )
#                     for _, group in inn_df.groupby("Partnership_Key"):
#                         runs = group["scrM_BatsmanRuns"].sum()
#                         extras = (
#                             group["scrM_ByeRuns"].sum() if "scrM_ByeRuns" in group else 0
#                         ) + (
#                             group["scrM_LegByeRuns"].sum() if "scrM_LegByeRuns" in group else 0
#                         ) + (
#                             group["scrM_NoBallRuns"].sum() if "scrM_NoBallRuns" in group else 0
#                         ) + (
#                             group["scrM_WideRuns"].sum() if "scrM_WideRuns" in group else 0
#                         ) + (
#                             group["scrM_PenaltyRuns"].sum() if "scrM_PenaltyRuns" in group else 0
#                         )
#                         partnerships.append(runs + extras)

#                 highest_pship = max(partnerships) if partnerships else 0

#                 inning_summary[inn] = {
#                     "total": total_runs,
#                     "overs": overs,
#                     "extras_total": extras_total,
#                     "nb": no_balls,
#                     "w": wide_balls,
#                     "b": byes,
#                     "lb": leg_byes,
#                     "crr": crr,
#                     "Wickets": wickets,
#                     "Balls": balls,
#                     "highest_pship": highest_pship
#                 }

#                 # --- Fall of Wickets ---
#                 wickets_df = inn_df[inn_df['scrM_IsWicket'] == 1]
#                 fow_list = []
#                 if not wickets_df.empty:
#                     for _, row in wickets_df.iterrows():
#                         runs_till_wicket = inn_df.loc[
#                             (inn_df['scrM_OverNo'] < row['scrM_OverNo']) |
#                             ((inn_df['scrM_OverNo'] == row['scrM_OverNo']) & (inn_df['scrM_DelNo'] <= row['scrM_DelNo']))
#                         ]['scrM_DelRuns'].sum()

#                         fow_list.append({
#                             "runs": runs_till_wicket,
#                             "wkt_no": len(fow_list) + 1,
#                             "over": f"{row['scrM_OverNo']}.{row['scrM_DelNo']}",
#                             "batter": row['scrM_PlayMIdStrikerName']
#                         })
#                 fall_of_wickets[inn] = fow_list

#                 # --- Bowler Scorecard ---
#                 bowlers = []
#                 for bowler, bdf in inn_df.groupby("scrM_PlayMIdBowlerName"):
#                     balls_bowled = len(bdf)
#                     runs_given = bdf["scrM_DelRuns"].sum()
#                     wickets_taken = bdf["scrM_IsWicket"].sum()
#                     dots = (bdf["scrM_BatsmanRuns"] == 0).sum()

#                     overs_bowled = f"{balls_bowled // 6}.{balls_bowled % 6}"
#                     econ = round(runs_given / (balls_bowled / 6), 2) if balls_bowled else 0

#                     real_bowler_id = (
#                         int(bdf["scrM_PlayMIdBowler"].iloc[0])
#                         if "scrM_PlayMIdBowler" in bdf.columns and not bdf["scrM_PlayMIdBowler"].isna().all()
#                         else None
#                     )

#                     bowlers.append({
#                         "name": bowler,
#                         "PlayMId": real_bowler_id,
#                         "MatchName": selected_match,
#                         "InningNo": inn,
#                         "overs": overs_bowled,
#                         "maidens": 0,
#                         "runs": runs_given,
#                         "wickets": wickets_taken,
#                         "econ": econ,
#                         "dots": dots,
#                         "bdry_pct": round(((bdf["scrM_BatsmanRuns"].isin([4,6]).sum()) / balls_bowled) * 100, 2) if balls_bowled else 0,
#                         "db_pct": round((dots / balls_bowled) * 100, 2) if balls_bowled else 0,
#                         "vs_batters": [
#                             {
#                                 "batter": batter,
#                                 "balls": len(vs_df),
#                                 "runs": vs_df["scrM_BatsmanRuns"].sum(),
#                                 "wickets": vs_df["scrM_IsWicket"].sum(),
#                                 "wd": vs_df["scrM_IsWideBall"].sum() if "scrM_IsWideBall" in vs_df.columns else 0,
#                                 "nb": vs_df["scrM_IsNoBall"].sum() if "scrM_IsNoBall" in vs_df.columns else 0,
#                                 "econ": round(vs_df["scrM_DelRuns"].sum() / (len(vs_df) / 6), 2) if len(vs_df) else 0,
#                                 "fours": (vs_df["scrM_BatsmanRuns"] == 4).sum(),
#                                 "sixes": (vs_df["scrM_BatsmanRuns"] == 6).sum()
#                             }
#                             for batter, vs_df in bdf.groupby("scrM_PlayMIdStrikerName")
#                         ]
#                     })
#                 bowler_scorecard[inn] = bowlers

#             # --- Partnership Charts ---
#             if match_innings:
#                 for inn in match_innings:
#                     inn_no = inn["Inn_Inning"]
#                     df = get_innings_deliveries(selected_match, inn_no)
#                     team_name = df["scrM_TeamName"].iloc[0] if not df.empty else f"Inning {inn_no}"
#                     fig = create_partnership_chart(df, team_name)
#                     chart_html = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')
#                     partnership_charts[inn_no] = chart_html

#             # ✅ Only build Multi-Day Report if format is multiday
#             # --- Multiday Report ---
#             # --- Multiday Report ---
#             # ✅ Only build Multi-Day Report if format is multiday
#             if match_format_code in [27, 29]:
#                 multi_day_report = generate_multi_day_report(ball_by_ball_df, match_innings)

#                 for day, day_payload in multi_day_report["days"].items():
#                     for sess_no, session_innings in day_payload["sessions"].items():
#                         for inn in session_innings:
#                             # ✅ Inning + Day + Session filter
#                             inn_df = ball_by_ball_df[
#                                 (ball_by_ball_df["scrM_InningNo"] == inn["innings"]) &
#                                 (ball_by_ball_df["scrM_DayNo"] == day) &
#                                 (ball_by_ball_df["scrM_SessionNo"] == sess_no)
#                             ].copy()

#                             # ✅ Pitch maps (LHB / RHB split)
#                             inn["pitchmaps"] = generate_session_pitchmaps(
#                                 ball_by_ball_df, day, inn["innings"], sess_no
#                             )

#                             # ✅ Batter table + Wagon wheels
#                             batters = []
#                             for batter, bdf in inn_df.groupby("scrM_PlayMIdStrikerName"):
#                                 runs = bdf["scrM_BatsmanRuns"].sum()
#                                 balls = len(bdf)
#                                 dots = (bdf["scrM_BatsmanRuns"] == 0).sum()
#                                 fours = (bdf["scrM_BatsmanRuns"] == 4).sum()
#                                 sixes = (bdf["scrM_BatsmanRuns"] == 6).sum()
#                                 ones = (bdf["scrM_BatsmanRuns"] == 1).sum()
#                                 twos = (bdf["scrM_BatsmanRuns"] == 2).sum()
#                                 threes = (bdf["scrM_BatsmanRuns"] == 3).sum()
#                                 sr = round((runs / balls) * 100, 2) if balls else 0

#                                 hand_val = bdf["BatterHand"].dropna().unique()
#                                 batter_hand = hand_val[0] if len(hand_val) > 0 else None
#                                 wagon_img = generate_wagon_wheel(bdf, batter_hand)

#                                 batters.append({
#                                     "name": batter,
#                                     "runs": runs,
#                                     "balls": balls,
#                                     "dots": dots,
#                                     "ones": ones,
#                                     "twos": twos,
#                                     "threes": threes,
#                                     "fours": fours,
#                                     "sixes": sixes,
#                                     "sr": sr,
#                                     "wagon_wheel": wagon_img
#                                 })
#                             inn["batter_table"] = batters

#                             # ✅ Bowler table
#                             bowlers = []
#                             for bowler, bdf in inn_df.groupby("scrM_PlayMIdBowlerName"):
#                                 balls_bowled = len(bdf)
#                                 runs_given = bdf["scrM_DelRuns"].sum()
#                                 wkts = bdf["scrM_IsWicket"].sum()
#                                 overs = f"{balls_bowled // 6}.{balls_bowled % 6}"
#                                 econ = round(runs_given / (balls_bowled / 6), 2) if balls_bowled else 0
#                                 wides = bdf["scrM_IsWideBall"].sum() if "scrM_IsWideBall" in bdf else 0
#                                 noballs = bdf["scrM_IsNoBall"].sum() if "scrM_IsNoBall" in bdf else 0

#                                 bowlers.append({
#                                     "name": bowler,
#                                     "overs": overs,
#                                     "maidens": 0,
#                                     "runs": runs_given,
#                                     "wkts": wkts,
#                                     "wb": wides,
#                                     "nb": noballs,
#                                     "econ": econ
#                                 })
#                             inn["bowler_table"] = bowlers

#                             # ✅ Radar charts (consolidated, spin, pace)
#                             # ✅ Radar charts (Consolidated, Spin, Pace)
#                             try:
#                                 radar_report = {
#                                     "Consolidated": generate_session_radar_chart(
#                                         ball_by_ball_df,
#                                         day,
#                                         inn["innings"],
#                                         sess_no,
#                                         team_name=inn.get("batting_team", "Team"),
#                                         bowler_type=None
#                                     ),
#                                     "Spin": generate_session_radar_chart(
#                                         ball_by_ball_df,
#                                         day,
#                                         inn["innings"],
#                                         sess_no,
#                                         team_name=inn.get("batting_team", "Team"),
#                                         bowler_type="Spin"
#                                     ),
#                                     "Pace": generate_session_radar_chart(
#                                         ball_by_ball_df,
#                                         day,
#                                         inn["innings"],
#                                         sess_no,
#                                         team_name=inn.get("batting_team", "Team"),
#                                         bowler_type="Pace"
#                                     )
#                                 }

#                                 inn["radar_chart"] = radar_report.get("Consolidated")
#                                 inn["spin_radar_chart"] = radar_report.get("Spin")
#                                 inn["pace_radar_chart"] = radar_report.get("Pace")

#                             except Exception as e:
#                                 print(f"⚠️ Radar chart failed for Inn {inn['innings']} Day {day} S{sess_no}: {e}")
#                                 inn["radar_chart"] = None
#                                 inn["spin_radar_chart"] = None
#                                 inn["pace_radar_chart"] = None


#                             # ✅ Partnership table
#                             partnerships = []
#                             if not inn_df.empty:
#                                 current_pair = []
#                                 runs_dict, balls_dict = {}, {}
#                                 total_runs, total_balls = 0, 0

#                                 for _, row in inn_df.iterrows():
#                                     striker = row.get("scrM_PlayMIdStrikerName", "Unknown")
#                                     non_striker = row.get("scrM_PlayMIdNonStrikerName", "Unknown")
#                                     batter_pair = [striker, non_striker]

#                                     # Update runs + balls for striker
#                                     runs_dict[striker] = runs_dict.get(striker, 0) + row.get("scrM_BatsmanRuns", 0)
#                                     balls_dict[striker] = balls_dict.get(striker, 0) + 1

#                                     # Count extras
#                                     extras = 0
#                                     for col in ["scrM_ByeRuns", "scrM_LegByeRuns",
#                                                 "scrM_NoBallRuns", "scrM_WideRuns", "scrM_PenaltyRuns"]:
#                                         if col in row and pd.notna(row[col]):
#                                             extras += row[col]

#                                     total_runs += row.get("scrM_BatsmanRuns", 0) + extras
#                                     total_balls += 1

#                                     # Detect new partnership
#                                     if not current_pair:
#                                         current_pair = batter_pair

#                                     if striker not in current_pair or non_striker not in current_pair:
#                                         # Save previous partnership
#                                         if len(current_pair) == 2:
#                                             b1, b2 = current_pair
#                                             partnerships.append({
#                                                 "batter1": f"{b1} {runs_dict.get(b1,0)} ({balls_dict.get(b1,0)})",
#                                                 "batter2": f"{b2} {runs_dict.get(b2,0)} ({balls_dict.get(b2,0)})",
#                                                 "runs_extras": total_runs,
#                                                 "balls": total_balls
#                                             })

#                                         # Reset for new pair
#                                         current_pair = batter_pair
#                                         runs_dict, balls_dict = {}, {}
#                                         total_runs, total_balls = 0, 0

#                                 # Save last partnership
#                                 if len(current_pair) == 2:
#                                     b1, b2 = current_pair
#                                     partnerships.append({
#                                         "batter1": f"{b1} {runs_dict.get(b1,0)} ({balls_dict.get(b1,0)})",
#                                         "batter2": f"{b2} {runs_dict.get(b2,0)} ({balls_dict.get(b2,0)})",
#                                         "runs_extras": total_runs,
#                                         "balls": total_balls
#                                     })

#                             inn["partnership_table"] = partnerships

#                             # ✅ Line & Length table
#                             inn["line_length_table"] = generate_line_length_table_new(
#                                 inn_df, day=day, inning=inn["innings"], session=sess_no
#                             )

#             # --- Limited Overs Report ---
#             # --- Limited Overs Report ---
#             if match_format_code in [26, 28] and len(match_innings) >= 2:

#                 def build_innings_report_from_df(inn_df, inn_obj):
#                     if inn_df.empty:
#                         return {}
#                     legal_balls = inn_df[inn_df["scrM_IsValidBall"] == 1]
#                     balls = len(legal_balls)
#                     overs = f"{balls // 6}.{balls % 6}" if balls else "0.0"

#                     runs = inn_df["scrM_DelRuns"].sum()
#                     run_rate = round(runs / (balls / 6), 2) if balls else 0

#                     fours = (inn_df["scrM_BatsmanRuns"] == 4).sum()
#                     sixes = (inn_df["scrM_BatsmanRuns"] == 6).sum()
#                     dot_balls = (inn_df["scrM_BatsmanRuns"] == 0).sum()
#                     ones = (inn_df["scrM_BatsmanRuns"] == 1).sum()
#                     twos = (inn_df["scrM_BatsmanRuns"] == 2).sum()
#                     threes = (inn_df["scrM_BatsmanRuns"] == 3).sum()

#                     wkts = inn_df["scrM_IsWicket"].sum()
#                     wides = inn_df["scrM_IsWideBall"].sum() if "scrM_IsWideBall" in inn_df else 0
#                     noballs = inn_df["scrM_IsNoBall"].sum() if "scrM_IsNoBall" in inn_df else 0
#                     byes = inn_df["scrM_ByeRuns"].sum() if "scrM_ByeRuns" in inn_df else 0
#                     legbyes = inn_df["scrM_LegByeRuns"].sum() if "scrM_LegByeRuns" in inn_df else 0
#                     penalty = inn_df["scrM_PenaltyRuns"].sum() if "scrM_PenaltyRuns" in inn_df else 0
#                     extras = wides + noballs + byes + legbyes + penalty

#                     eco_rate = round(runs / (balls / 6), 2) if balls else 0

#                     return {
#                         "name": inn_obj["TeamShortName"],
#                         "runs": runs,
#                         "score": f"{runs}/{wkts}",
#                         "run_rate": run_rate,
#                         "fours": fours,
#                         "sixes": sixes,
#                         "dot_balls": dot_balls,
#                         "ones": ones,
#                         "twos": twos,
#                         "threes": threes,
#                         "overs": overs,
#                         "maidens": 0,
#                         "wkts": wkts,
#                         "eco_rate": eco_rate,
#                         "wide_balls": wides,
#                         "no_balls": noballs,
#                         "extras": extras,
#                     }

#                 # Split match data into two innings
#                 innings1_df = ball_by_ball_df[ball_by_ball_df["scrM_InningNo"] == match_innings[0]["Inn_Inning"]]
#                 innings2_df = ball_by_ball_df[ball_by_ball_df["scrM_InningNo"] == match_innings[1]["Inn_Inning"]]

#                 # Define overs for different match formats
#                 if match_format_code == 28:  # T20
#                     phase_definitions = {
#                         "Overall": (1, 20),
#                         "Powerplay": (1, 6),
#                         "Middle Overs": (7, 15),
#                         "Slog Overs": (16, 20),
#                     }
#                 elif match_format_code == 26:  # ODI
#                     phase_definitions = {
#                         "Overall": (1, 50),
#                         "Powerplay": (1, 10),
#                         "Middle Overs": (11, 40),
#                         "Slog Overs": (41, 50),
#                     }
#                 else:
#                     phase_definitions = {"Overall": (1, 0)}

#                 phase_reports = {}

#                 # Iterate through each phase
#                 for phase_name, (start_over, end_over) in phase_definitions.items():
#                     inn1_phase = innings1_df[
#                         (innings1_df["scrM_OverNo"] >= start_over) &
#                         (innings1_df["scrM_OverNo"] <= end_over)
#                     ]
#                     inn2_phase = innings2_df[
#                         (innings2_df["scrM_OverNo"] >= start_over) &
#                         (innings2_df["scrM_OverNo"] <= end_over)
#                     ]

#                     if inn1_phase.empty and inn2_phase.empty:
#                         continue

#                     # Build reports for both innings
#                     team1_report = build_innings_report_from_df(inn1_phase, match_innings[0])
#                     team2_report = build_innings_report_from_df(inn2_phase, match_innings[1])

#                     # Team radar
#                     team1_radar, team2_radar = generate_team_comparison_radar(
#                         match_innings[0]["TeamShortName"],
#                         match_innings[1]["TeamShortName"],
#                         pd.concat([inn1_phase, inn2_phase])
#                     )

#                     # 🏏 Batting Charts
#                     fig_rpo = create_runs_per_over_chart(
#                         innings1_df, innings2_df,
#                         match_innings[0]["TeamShortName"], match_innings[1]["TeamShortName"],
#                         phase=phase_name
#                     )
#                     fig_rr = create_run_rate_chart(
#                         innings1_df, innings2_df,
#                         match_innings[0]["TeamShortName"], match_innings[1]["TeamShortName"],
#                         phase=phase_name
#                     )
#                     fig_donut = create_donut_charts(
#                         pd.concat([inn1_phase, inn2_phase]),
#                         match_innings[0]["TeamShortName"],
#                         match_innings[1]["TeamShortName"]
#                     )
#                     fig_extras = create_extra_runs_comparison_chart(
#                         pd.concat([inn1_phase, inn2_phase]),
#                         match_innings[0]["TeamShortName"],
#                         match_innings[1]["TeamShortName"]
#                     )
#                     fig_area = create_comparison_bar_chart(
#                         pd.concat([inn1_phase, inn2_phase]),
#                         match_innings[0]["TeamShortName"],
#                         match_innings[1]["TeamShortName"]
#                     )

#                     # Player contribution
#                     fig_player1 = create_player_contribution_donut(
#                         inn1_phase, match_innings[0]["TeamShortName"], 1, phase_name
#                     )
#                     fig_player2 = create_player_contribution_donut(
#                         inn2_phase, match_innings[1]["TeamShortName"], 2, phase_name
#                     )
#                     summary1 = generate_batting_summary(inn1_phase, match_innings[0]["TeamShortName"], 1, phase_name)
#                     summary2 = generate_batting_summary(inn2_phase, match_innings[1]["TeamShortName"], 2, phase_name)

#                     # Bowling donuts + summaries
#                     fig_bowl1 = create_bowling_dotball_donut(inn2_phase, match_innings[1]["TeamShortName"], 2, phase_name)
#                     fig_bowl2 = create_bowling_dotball_donut(inn1_phase, match_innings[0]["TeamShortName"], 1, phase_name)
#                     bowl_summary1 = build_bowling_summary(inn2_phase, match_innings[1]["TeamShortName"], 2, phase_name)
#                     bowl_summary2 = build_bowling_summary(inn1_phase, match_innings[0]["TeamShortName"], 1, phase_name)

#                     # Batting vs Pace & Spin
#                     pace_table1, spin_table1 = generate_batting_vs_pace_spin(
#                         inn1_phase, match_innings[0]["TeamShortName"], 1, phase_name
#                     )
#                     pace_table2, spin_table2 = generate_batting_vs_pace_spin(
#                         inn2_phase, match_innings[1]["TeamShortName"], 2, phase_name
#                     )
#                     vs_chart1 = create_vs_pace_spin_chart(inn1_phase, match_innings[0]["TeamShortName"], phase_name)
#                     vs_chart2 = create_vs_pace_spin_chart(inn2_phase, match_innings[1]["TeamShortName"], phase_name)

#                     # Partnership charts
#                     partnership_chart1 = create_phase_partnership_chart(
#                         inn1_phase, match_innings[0]["TeamShortName"], phase_name
#                     )
#                     partnership_chart2 = create_phase_partnership_chart(
#                         inn2_phase, match_innings[1]["TeamShortName"], phase_name
#                     )

#                     # 🆕 Bowling Variations
#                     delivery_chart_team1 = generate_delivery_type_distribution(
#                         inn1_phase, match_innings[0]["TeamShortName"], phase_name
#                     )
#                     pitch_chart_team1 = generate_pitch_area_distribution(
#                         inn1_phase, match_innings[0]["TeamShortName"], phase_name
#                     )
#                     delivery_chart_team2 = generate_delivery_type_distribution(
#                         inn2_phase, match_innings[1]["TeamShortName"], phase_name
#                     )
#                     pitch_chart_team2 = generate_pitch_area_distribution(
#                         inn2_phase, match_innings[1]["TeamShortName"], phase_name
#                     )

#                     # 🆕 Bowling: Split of Runs Conceded
#                     fig_runs_conceded_team1, table_data_team1 = create_runs_conceded_chart_and_table(
#                         inn2_phase, match_innings[1]["TeamShortName"], 2, phase_name
#                     )
#                     fig_runs_conceded_team2, table_data_team2 = create_runs_conceded_chart_and_table(
#                         inn1_phase, match_innings[0]["TeamShortName"], 1, phase_name
#                     )

#                     # 🆕 Bowling: Boundaries Conceded per Ball in Over
#                     fig_boundaries_team1, table_boundaries_team1 = create_boundaries_conceded_chart_and_table(
#                         inn2_phase, match_innings[0]["TeamShortName"], 2, phase_name  # <-- Team 0 here
#                     )
#                     fig_boundaries_team2, table_boundaries_team2 = create_boundaries_conceded_chart_and_table(
#                         inn1_phase, match_innings[1]["TeamShortName"], 1, phase_name  # <-- Team 1 here
#                     )
#                     # Build final phase report
#                     phase_reports[phase_name] = {
#                         "team1": team1_report,
#                         "team2": team2_report,
#                         "team1_radar_chart": team1_radar,
#                         "team2_radar_chart": team2_radar,

#                         "runs_per_over_chart": pio.to_html(fig_rpo, full_html=False, include_plotlyjs='cdn' if not phase_reports else False),
#                         "run_rate_chart": pio.to_html(fig_rr, full_html=False, include_plotlyjs=False),
#                         "donut_chart": pio.to_html(fig_donut, full_html=False, include_plotlyjs=False),
#                         "extra_runs_chart": pio.to_html(fig_extras, full_html=False, include_plotlyjs=False),
#                         "area_wise_chart": pio.to_html(fig_area, full_html=False, include_plotlyjs=False),

#                         "player_donut_team1": pio.to_html(fig_player1, full_html=False, include_plotlyjs=False),
#                         "player_donut_team2": pio.to_html(fig_player2, full_html=False, include_plotlyjs=False),
#                         "batting_summary_team1": summary1,
#                         "batting_summary_team2": summary2,

#                         "bowling_donut_team1": pio.to_html(fig_bowl1, full_html=False, include_plotlyjs=False),
#                         "bowling_donut_team2": pio.to_html(fig_bowl2, full_html=False, include_plotlyjs=False),
#                         "bowling_summary_team1": bowl_summary1,
#                         "bowling_summary_team2": bowl_summary2,

#                         # 🆕 Bowling: Split of Runs Conceded
#                         "runs_conceded_chart_team1": pio.to_html(fig_runs_conceded_team1, full_html=False, include_plotlyjs=False),
#                         "runs_conceded_chart_team2": pio.to_html(fig_runs_conceded_team2, full_html=False, include_plotlyjs=False),
#                         "runs_conceded_table_team1": table_data_team1,
#                         "runs_conceded_table_team2": table_data_team2,

#                         # 🆕 Bowling: Boundaries Conceded per Ball in Over
#                         "boundaries_chart_team1": pio.to_html(fig_boundaries_team1, full_html=False, include_plotlyjs=False),
#                         "boundaries_chart_team2": pio.to_html(fig_boundaries_team2, full_html=False, include_plotlyjs=False),
#                         "boundaries_table_team1": table_boundaries_team1,
#                         "boundaries_table_team2": table_boundaries_team2,

#                         "pace_table_team1": pace_table1,
#                         "spin_table_team1": spin_table1,
#                         "pace_table_team2": pace_table2,
#                         "spin_table_team2": spin_table2,
#                         "vs_chart_team1": pio.to_html(vs_chart1, full_html=False, include_plotlyjs=False),
#                         "vs_chart_team2": pio.to_html(vs_chart2, full_html=False, include_plotlyjs=False),

#                         "partnership_chart_team1": pio.to_html(partnership_chart1, full_html=False, include_plotlyjs=False),
#                         "partnership_chart_team2": pio.to_html(partnership_chart2, full_html=False, include_plotlyjs=False),

#                         # 🆕 Bowling Variations
#                         "delivery_chart_team1": pio.to_html(delivery_chart_team1, full_html=False, include_plotlyjs=False) if delivery_chart_team1 else None,
#                         "pitch_chart_team1": pio.to_html(pitch_chart_team1, full_html=False, include_plotlyjs=False) if pitch_chart_team1 else None,
#                         "delivery_chart_team2": pio.to_html(delivery_chart_team2, full_html=False, include_plotlyjs=False) if delivery_chart_team2 else None,
#                         "pitch_chart_team2": pio.to_html(pitch_chart_team2, full_html=False, include_plotlyjs=False) if pitch_chart_team2 else None,

#                         "team1_name": match_innings[0]["TeamShortName"],
#                         "team2_name": match_innings[1]["TeamShortName"],
#                     }




#     # ✅ Render template into HTML string
#     html_report = render_template(
#         "apps/apps-mailbox.html",
#         tournaments=tournaments,
#         teams=teams,
#         matches=matches,
#         selected_tournament=selected_tournament,
#         selected_team=selected_team,
#         selected_match=selected_match,
#         match_header=match_header,
#         match_innings=match_innings,
#         last_12_balls=last_12_balls,
#         scorecard_data=scorecard_data,
#         inning_summary=inning_summary,
#         fall_of_wickets=fall_of_wickets,
#         bowler_scorecard=bowler_scorecard,
#         partnership_charts=partnership_charts,
#         multi_day_report=multi_day_report,
#         limited_overs_report=limited_overs_report,
#         runs_per_over_chart=runs_per_over_chart,
#         run_rate_chart=run_rate_chart,
#         donut_chart=donut_chart,
#         extra_runs_chart=extra_runs_chart,
#         area_wise_chart=area_wise_chart,
#         phase_reports=phase_reports,
#         match_format_code=match_format_code
#     )

#     # ✅ Save HTML in cache for PDF export
#     global match_report_cache
#     if selected_match:
#         match_report_cache[selected_match] = html_report

#     return html_report

@apps.route('/apps/apps-report', methods=['GET', 'POST'])
@login_required
def match_reports():
    # 🆕 Filter tournaments by logged-in user's association
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    tournaments = get_all_tournaments(association_id)

    # ✅ Form selections (works for both GET and POST)
    selected_tournament = request.values.get("tournament")
    selected_team = request.values.get("team")
    selected_match = request.values.get("match")

    # debug: log incoming request and selection
    print("=== match_reports called ===")
    print("request.values:", dict(request.values))
    print("selected_tournament:", selected_tournament, "selected_team:", selected_team, "selected_match:", selected_match)

    # Dependent dropdowns
    teams = get_teams_by_tournament(selected_tournament) if selected_tournament else []
    matches = get_matches_by_team(selected_team, selected_tournament) if selected_team else []


    # Default objects
    match_header, match_innings, last_12_balls = None, [], []
    scorecard_data, inning_summary, fall_of_wickets, bowler_scorecard = {}, {}, {}, {}
    partnership_charts = {}
    multi_day_report, limited_overs_report = None, None
    runs_per_over_chart, run_rate_chart = None, None

    # ✅ initialize charts to None so they exist
    donut_chart = None
    extra_runs_chart = None
    area_wise_chart = None
    phase_reports = {}

    # Detect match format
    match_format_code = None
    if selected_tournament:
        try:
            match_format_code = get_match_format_code_by_tournament(selected_tournament)
        except Exception as e:
            print("⚠️ Could not fetch match format from primary source:", e)

    # --- helper helpers to make format checks robust ----------------
    def _is_multiday(fmt):
        """Return True if fmt indicates a multi-day/test style match."""
        if fmt is None:
            return False
        # numeric codes from DB (some systems use ints like 27/29)
        try:
            if str(fmt).isdigit():
                return int(fmt) in (27, 29)
        except Exception:
            pass
        s = str(fmt).lower()
        # fallbacks for string results from get_match_format_by_tournament
        return any(x in s for x in ("test", "multiday", "multi", "day", "4day", "5day"))

    def _is_limited_overs(fmt):
        """Return True if fmt indicates a limited overs match (T20/ODI/T10 etc)."""
        if fmt is None:
            return False

        # numeric codes from DB
        try:
            if str(fmt).isdigit():
                return int(fmt) in (26, 28, 167)   # ✅ ODI, T20, T10
        except Exception:
            pass

        # string fallbacks
        s = str(fmt).strip().lower()

        return any(x in s for x in (
            "t10", "10",          # ✅ ADD THIS
            "t20", "20",
            "odi", "one day", "one-day",
            "limited"
        ))

    # ----------------------------------------------------------------

    # fallback: if the code fetch failed earlier, try string-format lookup
    if match_format_code is None and selected_tournament:
        try:
            # optional helper (if you have a different function name, change accordingly)
            fallback_fmt = None
            try:
                fallback_fmt = get_match_format_by_tournament(selected_tournament)
            except Exception:
                # if helper not available or fails, ignore
                fallback_fmt = None

            if fallback_fmt:
                match_format_code = fallback_fmt  # keep string fallback — helpers above handle it
                print("match_reports: fallback match_format_code ->", match_format_code)
        except Exception as e:
            print("match_reports: fallback get_match_format_by_tournament failed:", e)

    # If match is selected → fetch details
    if selected_match:
        match_header = get_match_header(selected_match)
        match_innings = get_match_innings(selected_match)

        # ✅ FIX: ensure overs display shows balls too (3.4 instead of 4.0)
        for inn in match_innings:
            try:
                overs = int(float(inn.get("Inn_Overs") or 0))
            except Exception:
                overs = 0

            try:
                balls = int(float(inn.get("Inn_DeliveriesOfLastIncompleteOver") or 0))
            except Exception:
                balls = 0

            # if balls exist, show overs.balls else only overs
            inn["Inn_OversDisplay"] = f"{overs}.{balls}" if balls > 0 else str(overs)


        ball_by_ball_df = get_ball_by_ball_data(selected_match)

        print("ball_by_ball_df empty?:", getattr(ball_by_ball_df, "empty", True))
        try:
            print("ball_by_ball_df rows:", len(ball_by_ball_df))
        except Exception:
            pass
        print("match_format_code (before fallback):", match_format_code)

        # --- Multi-day report (robust detection) ---
        if not getattr(ball_by_ball_df, "empty", True) and _is_multiday(match_format_code):
            try:
                multi_day_report = generate_multi_day_report(ball_by_ball_df, match_innings)

                for day, day_payload in multi_day_report["days"].items():
                    for sess_no, session_innings in day_payload["sessions"].items():
                        for inn in session_innings:
                            # ✅ Inning + Day + Session filter
                            inn_df = ball_by_ball_df[
                                (ball_by_ball_df["scrM_InningNo"] == inn["innings"]) &
                                (ball_by_ball_df["scrM_DayNo"] == day) &
                                (ball_by_ball_df["scrM_SessionNo"] == sess_no)
                            ].copy()

                            # ✅ Pitch maps (LHB / RHB split)
                            inn["pitchmaps"] = generate_session_pitchmaps(
                                ball_by_ball_df, day, inn["innings"], sess_no
                            )

                            # ✅ Batter table + Wagon wheels
                            batters = []
                            for batter, bdf in inn_df.groupby("scrM_PlayMIdStrikerName"):
                                runs = bdf["scrM_BatsmanRuns"].sum()
                                balls = len(bdf)
                                dots = (bdf["scrM_BatsmanRuns"] == 0).sum()
                                fours = (bdf["scrM_BatsmanRuns"] == 4).sum()
                                sixes = (bdf["scrM_BatsmanRuns"] == 6).sum()
                                ones = (bdf["scrM_BatsmanRuns"] == 1).sum()
                                twos = (bdf["scrM_BatsmanRuns"] == 2).sum()
                                threes = (bdf["scrM_BatsmanRuns"] == 3).sum()
                                sr = round((runs / balls) * 100, 2) if balls else 0

                                hand_val = bdf["BatterHand"].dropna().unique() if "BatterHand" in bdf else []
                                batter_hand = hand_val[0] if len(hand_val) > 0 else None
                                wagon_img = generate_wagon_wheel(bdf, batter_hand)

                                batters.append({
                                    "name": batter,
                                    "runs": runs,
                                    "balls": balls,
                                    "dots": dots,
                                    "ones": ones,
                                    "twos": twos,
                                    "threes": threes,
                                    "fours": fours,
                                    "sixes": sixes,
                                    "sr": sr,
                                    "wagon_wheel": wagon_img
                                })
                            inn["batter_table"] = batters

                            # ✅ Bowler table
                            bowlers = []
                            for bowler, bdf in inn_df.groupby("scrM_PlayMIdBowlerName"):
                                balls_bowled = len(bdf)
                                runs_given = bdf["scrM_DelRuns"].sum()
                                wkts = bdf["scrM_IsWicket"].sum()
                                overs = f"{balls_bowled // 6}.{balls_bowled % 6}"
                                econ = round(runs_given / (balls_bowled / 6), 2) if balls_bowled else 0
                                wides = bdf["scrM_IsWideBall"].sum() if "scrM_IsWideBall" in bdf else 0
                                noballs = bdf["scrM_IsNoBall"].sum() if "scrM_IsNoBall" in bdf else 0

                                bowlers.append({
                                    "name": bowler,
                                    "overs": overs,
                                    "maidens": 0,
                                    "runs": runs_given,
                                    "wkts": wkts,
                                    "wb": wides,
                                    "nb": noballs,
                                    "econ": econ
                                })
                            inn["bowler_table"] = bowlers

                            # ✅ Radar charts (consolidated, spin, pace)
                            try:
                                radar_report = {
                                    "Consolidated": generate_session_radar_chart(
                                        ball_by_ball_df,
                                        day,
                                        inn["innings"],
                                        sess_no,
                                        team_name=inn.get("batting_team", "Team"),
                                        bowler_type=None
                                    ),
                                    "Spin": generate_session_radar_chart(
                                        ball_by_ball_df,
                                        day,
                                        inn["innings"],
                                        sess_no,
                                        team_name=inn.get("batting_team", "Team"),
                                        bowler_type="Spin"
                                    ),
                                    "Pace": generate_session_radar_chart(
                                        ball_by_ball_df,
                                        day,
                                        inn["innings"],
                                        sess_no,
                                        team_name=inn.get("batting_team", "Team"),
                                        bowler_type="Pace"
                                    )
                                }

                                inn["radar_chart"] = radar_report.get("Consolidated")
                                inn["spin_radar_chart"] = radar_report.get("Spin")
                                inn["pace_radar_chart"] = radar_report.get("Pace")

                            except Exception as e:
                                print(f"⚠️ Radar chart failed for Inn {inn['innings']} Day {day} S{sess_no}: {e}")
                                inn["radar_chart"] = None
                                inn["spin_radar_chart"] = None
                                inn["pace_radar_chart"] = None

                            # ✅ Partnership table
                            partnerships = []
                            if not inn_df.empty:
                                current_pair = []
                                runs_dict, balls_dict = {}, {}
                                total_runs, total_balls = 0, 0

                                for _, row in inn_df.iterrows():
                                    striker = row.get("scrM_PlayMIdStrikerName", "Unknown")
                                    non_striker = row.get("scrM_PlayMIdNonStrikerName", "Unknown")
                                    batter_pair = [striker, non_striker]

                                    # Update runs + balls for striker
                                    runs_dict[striker] = runs_dict.get(striker, 0) + row.get("scrM_BatsmanRuns", 0)
                                    balls_dict[striker] = balls_dict.get(striker, 0) + 1

                                    # Count extras
                                    extras = 0
                                    for col in ["scrM_ByeRuns", "scrM_LegByeRuns",
                                                "scrM_NoBallRuns", "scrM_WideRuns", "scrM_PenaltyRuns"]:
                                        if col in row and pd.notna(row[col]):
                                            extras += row[col]

                                    total_runs += row.get("scrM_BatsmanRuns", 0) + extras
                                    total_balls += 1

                                    # Detect new partnership
                                    if not current_pair:
                                        current_pair = batter_pair

                                    if striker not in current_pair or non_striker not in current_pair:
                                        # Save previous partnership
                                        if len(current_pair) == 2:
                                            b1, b2 = current_pair
                                            partnerships.append({
                                                "batter1": f"{b1} {runs_dict.get(b1,0)} ({balls_dict.get(b1,0)})",
                                                "batter2": f"{b2} {runs_dict.get(b2,0)} ({balls_dict.get(b2,0)})",
                                                "runs_extras": total_runs,
                                                "balls": total_balls
                                            })

                                        # Reset for new pair
                                        current_pair = batter_pair
                                        runs_dict, balls_dict = {}, {}
                                        total_runs, total_balls = 0, 0

                                # Save last partnership
                                if len(current_pair) == 2:
                                    b1, b2 = current_pair
                                    partnerships.append({
                                        "batter1": f"{b1} {runs_dict.get(b1,0)} ({balls_dict.get(b1,0)})",
                                        "batter2": f"{b2} {runs_dict.get(b2,0)} ({balls_dict.get(b2,0)})",
                                        "runs_extras": total_runs,
                                        "balls": total_balls
                                    })

                            inn["partnership_table"] = partnerships

                            # ✅ Line & Length table
                            inn["line_length_table"] = generate_line_length_table_new(
                                inn_df, day=day, inning=inn["innings"], session=sess_no
                            )

            except Exception as e:
                print("⚠️ generate_multi_day_report failed:", e)
                multi_day_report = None

        # --- Limited Overs Report (robust detection) ---
        if not getattr(ball_by_ball_df, "empty", True) and _is_limited_overs(match_format_code) and len(match_innings) >= 2:
            try:

                def build_innings_report_from_df(inn_df, inn_obj):
                    if inn_df.empty:
                        return {}
                    legal_balls = inn_df[inn_df["scrM_IsValidBall"] == 1] if "scrM_IsValidBall" in inn_df else inn_df
                    balls = len(legal_balls)
                    overs = f"{balls // 6}.{balls % 6}" if balls else "0.0"

                    runs = inn_df["scrM_DelRuns"].sum() if "scrM_DelRuns" in inn_df else 0
                    run_rate = round(runs / (balls / 6), 2) if balls else 0

                    fours = (inn_df["scrM_BatsmanRuns"] == 4).sum() if "scrM_BatsmanRuns" in inn_df else 0
                    sixes = (inn_df["scrM_BatsmanRuns"] == 6).sum() if "scrM_BatsmanRuns" in inn_df else 0
                    dot_balls = (inn_df["scrM_BatsmanRuns"] == 0).sum() if "scrM_BatsmanRuns" in inn_df else 0
                    ones = (inn_df["scrM_BatsmanRuns"] == 1).sum() if "scrM_BatsmanRuns" in inn_df else 0
                    twos = (inn_df["scrM_BatsmanRuns"] == 2).sum() if "scrM_BatsmanRuns" in inn_df else 0
                    threes = (inn_df["scrM_BatsmanRuns"] == 3).sum() if "scrM_BatsmanRuns" in inn_df else 0

                    wkts = inn_df["scrM_IsWicket"].sum() if "scrM_IsWicket" in inn_df else 0
                    wides = inn_df["scrM_IsWideBall"].sum() if "scrM_IsWideBall" in inn_df else 0
                    noballs = inn_df["scrM_IsNoBall"].sum() if "scrM_IsNoBall" in inn_df else 0
                    byes = inn_df["scrM_ByeRuns"].sum() if "scrM_ByeRuns" in inn_df else 0
                    legbyes = inn_df["scrM_LegByeRuns"].sum() if "scrM_LegByeRuns" in inn_df else 0
                    penalty = inn_df["scrM_PenaltyRuns"].sum() if "scrM_PenaltyRuns" in inn_df else 0
                    extras = wides + noballs + byes + legbyes + penalty

                    eco_rate = round(runs / (balls / 6), 2) if balls else 0

                    return {
                        "name": inn_obj.get("TeamShortName", ""),
                        "runs": runs,
                        "score": f"{runs}/{wkts}",
                        "run_rate": run_rate,
                        "fours": fours,
                        "sixes": sixes,
                        "dot_balls": dot_balls,
                        "ones": ones,
                        "twos": twos,
                        "threes": threes,
                        "overs": overs,
                        "maidens": 0,
                        "wkts": wkts,
                        "eco_rate": eco_rate,
                        "wide_balls": wides,
                        "no_balls": noballs,
                        "extras": extras,
                    }

                # Split match data into two innings
                innings1_df = ball_by_ball_df[ball_by_ball_df["scrM_InningNo"] == match_innings[0]["Inn_Inning"]]
                innings2_df = ball_by_ball_df[ball_by_ball_df["scrM_InningNo"] == match_innings[1]["Inn_Inning"]]

                # Define overs for different match formats
                if str(match_format_code).isdigit() and int(match_format_code) == 28:
                    # T20
                    phase_definitions = {
                        "Overall": (1, 20),
                        "Powerplay": (1, 6),
                        "Middle Overs": (7, 15),
                        "Slog Overs": (16, 20),
                    }
                elif str(match_format_code).isdigit() and int(match_format_code) == 167:
                    # ✅ T10
                    phase_definitions = {
                        "Overall": (1, 10),
                        "Powerplay": (1, 2),
                        "Middle Overs": (3, 7),
                        "Slog Overs": (8, 10),
                    }
                elif str(match_format_code).isdigit() and int(match_format_code) == 26:
                    # ODI
                    phase_definitions = {
                        "Overall": (1, 50),
                        "Powerplay": (1, 10),
                        "Middle Overs": (11, 40),
                        "Slog Overs": (41, 50),
                    }
                else:
                    # fallback for string codes like 't20' or 'odi'
                    s_fmt = str(match_format_code).lower() if match_format_code else ""
                    if "t10" in s_fmt or "10" in s_fmt:
                        phase_definitions = {
                            "Overall": (1, 10),
                            "Powerplay": (1, 2),
                            "Middle Overs": (3, 7),
                            "Slog Overs": (8, 10),
                        }
                    elif "t20" in s_fmt or "20" in s_fmt:
                        phase_definitions = {
                            "Overall": (1, 20),
                            "Powerplay": (1, 6),
                            "Middle Overs": (7, 15),
                            "Slog Overs": (16, 20),
                        }
                    elif "odi" in s_fmt or "50" in s_fmt or "one day" in s_fmt:
                        phase_definitions = {
                            "Overall": (1, 50),
                            "Powerplay": (1, 10),
                            "Middle Overs": (11, 40),
                            "Slog Overs": (41, 50),
                        }
                    else:
                        phase_definitions = {"Overall": (1, 0)}

                phase_reports = {}

                # Iterate through each phase
                for phase_name, (start_over, end_over) in phase_definitions.items():
                    inn1_phase = innings1_df[
                        (innings1_df["scrM_OverNo"] >= start_over) &
                        (innings1_df["scrM_OverNo"] <= end_over)
                    ] if end_over and end_over > 0 else innings1_df

                    inn2_phase = innings2_df[
                        (innings2_df["scrM_OverNo"] >= start_over) &
                        (innings2_df["scrM_OverNo"] <= end_over)
                    ] if end_over and end_over > 0 else innings2_df

                    if inn1_phase.empty and inn2_phase.empty:
                        continue

                    # Build reports for both innings
                    team1_report = build_innings_report_from_df(inn1_phase, match_innings[0])
                    team2_report = build_innings_report_from_df(inn2_phase, match_innings[1])

                    # Team radar
                    team1_radar, team2_radar = generate_team_comparison_radar(
                        match_innings[0].get("TeamShortName", ""),
                        match_innings[1].get("TeamShortName", ""),
                        pd.concat([inn1_phase, inn2_phase]) if not inn1_phase.empty or not inn2_phase.empty else pd.DataFrame()
                    )

                    # 🏏 Batting Charts
                    fig_rpo = create_runs_per_over_chart(
                        innings1_df, innings2_df,
                        match_innings[0].get("TeamShortName", ""), match_innings[1].get("TeamShortName", ""),
                        phase=phase_name
                    )
                    fig_rr = create_run_rate_chart(
                        innings1_df, innings2_df,
                        match_innings[0].get("TeamShortName", ""), match_innings[1].get("TeamShortName", ""),
                        phase=phase_name
                    )
                    fig_donut = create_donut_charts(
                        pd.concat([inn1_phase, inn2_phase]) if (not inn1_phase.empty or not inn2_phase.empty) else pd.DataFrame(),
                        match_innings[0].get("TeamShortName", ""),
                        match_innings[1].get("TeamShortName", "")
                    )
                    fig_extras = create_extra_runs_comparison_chart(
                        pd.concat([inn1_phase, inn2_phase]) if (not inn1_phase.empty or not inn2_phase.empty) else pd.DataFrame(),
                        match_innings[0].get("TeamShortName", ""),
                        match_innings[1].get("TeamShortName", "")
                    )
                    fig_area = create_comparison_bar_chart(
                        pd.concat([inn1_phase, inn2_phase]) if (not inn1_phase.empty or not inn2_phase.empty) else pd.DataFrame(),
                        match_innings[0].get("TeamShortName", ""),
                        match_innings[1].get("TeamShortName", "")
                    )

                    # Player contribution
                    fig_player1 = create_player_contribution_donut(
                        inn1_phase, match_innings[0].get("TeamShortName", ""), 1, phase_name
                    )
                    fig_player2 = create_player_contribution_donut(
                        inn2_phase, match_innings[1].get("TeamShortName", ""), 2, phase_name
                    )
                    summary1 = generate_batting_summary(inn1_phase, match_innings[0].get("TeamShortName", ""), 1, phase_name)
                    summary2 = generate_batting_summary(inn2_phase, match_innings[1].get("TeamShortName", ""), 2, phase_name)

                    # Bowling donuts + summaries
                    fig_bowl1 = create_bowling_dotball_donut(inn2_phase, match_innings[1].get("TeamShortName", ""), 2, phase_name)
                    fig_bowl2 = create_bowling_dotball_donut(inn1_phase, match_innings[0].get("TeamShortName", ""), 1, phase_name)
                    bowl_summary1 = build_bowling_summary(inn2_phase, match_innings[1].get("TeamShortName", ""), 2, phase_name)
                    bowl_summary2 = build_bowling_summary(inn1_phase, match_innings[0].get("TeamShortName", ""), 1, phase_name)

                    # Batting vs Pace & Spin
                    pace_table1, spin_table1 = generate_batting_vs_pace_spin(
                        inn1_phase, match_innings[0].get("TeamShortName", ""), 1, phase_name
                    )
                    pace_table2, spin_table2 = generate_batting_vs_pace_spin(
                        inn2_phase, match_innings[1].get("TeamShortName", ""), 2, phase_name
                    )
                    vs_chart1 = create_vs_pace_spin_chart(inn1_phase, match_innings[0].get("TeamShortName", ""), phase_name)
                    vs_chart2 = create_vs_pace_spin_chart(inn2_phase, match_innings[1].get("TeamShortName", ""), phase_name)

                    # Partnership charts
                    partnership_chart1 = create_phase_partnership_chart(
                        inn1_phase, match_innings[0].get("TeamShortName", ""), phase_name
                    )
                    partnership_chart2 = create_phase_partnership_chart(
                        inn2_phase, match_innings[1].get("TeamShortName", ""), phase_name
                    )

                    # 🆕 Bowling Variations
                    delivery_chart_team1 = generate_delivery_type_distribution(
                        inn1_phase, match_innings[0].get("TeamShortName", ""), phase_name
                    )
                    pitch_chart_team1 = generate_pitch_area_distribution(
                        inn1_phase, match_innings[0].get("TeamShortName", ""), phase_name
                    )
                    delivery_chart_team2 = generate_delivery_type_distribution(
                        inn2_phase, match_innings[1].get("TeamShortName", ""), phase_name
                    )
                    pitch_chart_team2 = generate_pitch_area_distribution(
                        inn2_phase, match_innings[1].get("TeamShortName", ""), phase_name
                    )

                    # 🆕 Bowling: Split of Runs Conceded
                    fig_runs_conceded_team1, table_data_team1 = create_runs_conceded_chart_and_table(
                        inn2_phase, match_innings[1].get("TeamShortName", ""), 2, phase_name
                    )
                    fig_runs_conceded_team2, table_data_team2 = create_runs_conceded_chart_and_table(
                        inn1_phase, match_innings[0].get("TeamShortName", ""), 1, phase_name
                    )

                    # 🆕 Bowling: Boundaries Conceded per Ball in Over
                    fig_boundaries_team1, table_boundaries_team1 = create_boundaries_conceded_chart_and_table(
                        inn2_phase, match_innings[0].get("TeamShortName", ""), 2, phase_name
                    )
                    fig_boundaries_team2, table_boundaries_team2 = create_boundaries_conceded_chart_and_table(
                        inn1_phase, match_innings[1].get("TeamShortName", ""), 1, phase_name
                    )

                    # Build final phase report
                    phase_reports[phase_name] = {
                        "team1": team1_report,
                        "team2": team2_report,
                        "team1_radar_chart": team1_radar,
                        "team2_radar_chart": team2_radar,

                        "runs_per_over_chart": pio.to_html(
                            fig_rpo,
                            full_html=False,
                            include_plotlyjs='cdn' if not phase_reports else False,
                            config={'responsive': False}  # 👈 disable auto-shrink
                        ),

                        "run_rate_chart": pio.to_html(
                            fig_rr,
                            full_html=False,
                            include_plotlyjs=False,
                            config={'responsive': False}  # 👈 disable auto-shrink
                        ),
                        "donut_chart": pio.to_html(fig_donut, full_html=False, include_plotlyjs=False),
                        "extra_runs_chart": pio.to_html(fig_extras, full_html=False, include_plotlyjs=False),
                        "area_wise_chart": pio.to_html(
                            fig_area,
                            full_html=False,
                            include_plotlyjs=False,
                            config={'responsive': False}  # 👈 stop Plotly from shrinking
                        ),


                        "player_donut_team1": pio.to_html(fig_player1, full_html=False, include_plotlyjs=False),
                        "player_donut_team2": pio.to_html(fig_player2, full_html=False, include_plotlyjs=False),
                        "batting_summary_team1": summary1,
                        "batting_summary_team2": summary2,

                        "bowling_donut_team1": pio.to_html(fig_bowl1, full_html=False, include_plotlyjs=False),
                        "bowling_donut_team2": pio.to_html(fig_bowl2, full_html=False, include_plotlyjs=False),
                        "bowling_summary_team1": bowl_summary1,
                        "bowling_summary_team2": bowl_summary2,

                        # 🆕 Bowling: Split of Runs Conceded
                        # 🆕 Bowling: Split of Runs Conceded
                        "runs_conceded_chart_team1": (
                            pio.to_html(
                                fig_runs_conceded_team1,
                                full_html=False,
                                include_plotlyjs=False,
                                config={"displayModeBar": False, "responsive": True}
                            )
                            if fig_runs_conceded_team1 is not None
                            else ""
                        ),
                        "runs_conceded_chart_team2": (
                            pio.to_html(
                                fig_runs_conceded_team2,
                                full_html=False,
                                include_plotlyjs=False,
                                config={"displayModeBar": False, "responsive": True}
                            )
                            if fig_runs_conceded_team2 is not None
                            else ""
                        ),

                        "runs_conceded_table_team1": table_data_team1,
                        "runs_conceded_table_team2": table_data_team2,

                        # 🆕 Bowling: Boundaries Conceded per Ball in Over
                        "boundaries_chart_team1": pio.to_html(
                            fig_boundaries_team1,
                            full_html=False,
                            include_plotlyjs=False,
                            config={'responsive': False}
                        ),
                        "boundaries_chart_team2": pio.to_html(
                            fig_boundaries_team2,
                            full_html=False,
                            include_plotlyjs=False,
                            config={'responsive': False}
                        ),

                        "boundaries_table_team1": table_boundaries_team1,
                        "boundaries_table_team2": table_boundaries_team2,

                        "pace_table_team1": pace_table1,
                        "spin_table_team1": spin_table1,
                        "pace_table_team2": pace_table2,
                        "spin_table_team2": spin_table2,
                        "vs_chart_team1": pio.to_html(vs_chart1, full_html=False, include_plotlyjs=False),
                        "vs_chart_team2": pio.to_html(vs_chart2, full_html=False, include_plotlyjs=False),

                        "partnership_chart_team1": pio.to_html(partnership_chart1, full_html=False, include_plotlyjs=False),
                        "partnership_chart_team2": pio.to_html(partnership_chart2, full_html=False, include_plotlyjs=False),

                        # 🆕 Bowling Variations
                        "delivery_chart_team1": pio.to_html(delivery_chart_team1, full_html=False, include_plotlyjs=False) if delivery_chart_team1 else None,
                        "pitch_chart_team1": pio.to_html(pitch_chart_team1, full_html=False, include_plotlyjs=False) if pitch_chart_team1 else None,
                        "delivery_chart_team2": pio.to_html(delivery_chart_team2, full_html=False, include_plotlyjs=False) if delivery_chart_team2 else None,
                        "pitch_chart_team2": pio.to_html(pitch_chart_team2, full_html=False, include_plotlyjs=False) if pitch_chart_team2 else None,

                        "team1_name": match_innings[0].get("TeamShortName", ""),
                        "team2_name": match_innings[1].get("TeamShortName", ""),
                    }

            except Exception as e:
                print("⚠️ limited overs report failed:", e)
                limited_overs_report = None

    # ✅ Render template into HTML string (added view_type to context for template determinism)
    html_report = render_template(
        "apps/apps-mailbox.html",
        tournaments=tournaments,
        teams=teams,
        matches=matches,
        selected_tournament=selected_tournament,
        selected_team=selected_team,
        selected_match=selected_match,
        match_header=match_header,
        match_innings=match_innings,
        scorecard_data=scorecard_data,
        inning_summary=inning_summary,
        fall_of_wickets=fall_of_wickets,
        bowler_scorecard=bowler_scorecard,
        partnership_charts=partnership_charts,
        multi_day_report=multi_day_report,
        limited_overs_report=limited_overs_report,
        runs_per_over_chart=runs_per_over_chart,
        run_rate_chart=run_rate_chart,
        donut_chart=donut_chart,
        extra_runs_chart=extra_runs_chart,
        area_wise_chart=area_wise_chart,
        phase_reports=phase_reports,
        match_format_code=match_format_code,
        view_type=request.values.get("view_type", "report")
    )

    # ✅ Save HTML in cache for PDF export
    global match_report_cache
    if selected_match:
        match_report_cache[selected_match] = html_report

    return html_report


from flask import jsonify, abort
import os
import sys
import webbrowser
from playwright.sync_api import sync_playwright

from flask import jsonify, abort
import os, sys, webbrowser
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

@apps.route("/download_report_pdf/<string:match_id>")
def download_report_pdf(match_id):
    global match_report_cache  

    # Fetch cached full HTML page
    full_html = match_report_cache.get(match_id)
    if not full_html:
        abort(404, "No data found for this match report")

    # ✅ Extract only the report card div
    soup = BeautifulSoup(full_html, "html.parser")
    report_card = soup.select_one("#report-card")
    if not report_card:
        abort(404, "Report card not found in cached HTML")

    # ✅ Inline CSS
    inline_css = """
    <style>
      body {
        background:#132337;
        margin:0;
        padding:24px;
        font-family:ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, Noto Sans;
        color:#cbd5e1;
      }
      .outer-card {
        background:#132337;
        border-radius:12px;
        padding:20px;
        box-shadow:0 2px 6px rgba(0,0,0,.5);
      }
      table {
        width:100%;
        border-collapse: collapse;
        margin-top:16px;
        font-size:14px;
      }
      th, td {
        padding:10px 12px;
        text-align:center;
        border:1px solid #1B355B;
      }
      th { background:#1B355B; color:#fff; }
      td { background:#1E3D6C; color:#f1f5f9; }
      tr:nth-child(even) td { background:#24497A; }
    </style>
    """

    # ✅ Minimal HTML wrapper (only report card)
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <title>Match Report</title>
      {inline_css}
      <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
      <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    </head>
    <body>
      <div class="outer-card">
        {str(report_card)}
      </div>
    </body>
    </html>
    """

    downloads_folder = get_downloads_folder()
    pdf_path = os.path.join(downloads_folder, f"{match_id}_report.pdf")

    try:
        with sync_playwright() as p:
            # ✅ handle EXE vs DEV mode
            if getattr(sys, "frozen", False):
                base_path = os.path.join(sys._MEIPASS, "ms-playwright")
                chromium_path = None
                # 🔍 auto-detect chromium folder
                for folder in os.listdir(base_path):
                    if folder.startswith("chromium-"):
                        chromium_path = os.path.join(base_path, folder, "chrome-win", "chrome.exe")
                        break
                if not chromium_path or not os.path.exists(chromium_path):
                    raise RuntimeError("Chromium executable not found in bundled ms-playwright")
                browser = p.chromium.launch(executable_path=chromium_path)
            else:
                browser = p.chromium.launch()

            page = browser.new_page()
            page.set_content(html_content, wait_until="networkidle")

            # ✅ Wait for charts to load
            page.wait_for_selector("#report-card", timeout=10000)
            try:
                page.wait_for_selector(".apexcharts-canvas", timeout=5000)
            except:
                pass
            try:
                page.wait_for_selector(".js-plotly-plot", timeout=5000)
            except:
                pass
            page.wait_for_timeout(1000)

            # ✅ Export PDF
            pdf_bytes = page.pdf(
                format="A4",
                landscape=True,
                print_background=True,
                prefer_css_page_size=True
            )
            browser.close()

        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)

    except Exception as e:
        print("⚠️ Playwright PDF export failed:", e)
        abort(500, "PDF generation failed")

    # ✅ Send the generated PDF file for download
    return send_file(
        pdf_path,
        as_attachment=True,
        download_name=f"{match_id}_report.pdf",
        mimetype="application/pdf"
    )









@apps.route('/apps/calendar', methods=['GET', 'POST'])
def calendar_default_view():
    import pandas as pd
    from flask import session
    from flask_login import current_user

    # 🆕 Filter tournaments by logged-in user's association
    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    # ==========================================================
    # ✅ TOURNAMENT DROPDOWN (value=id, label=name)
    # ==========================================================
    tournaments_raw = get_all_tournaments(association_id) or []
    tournaments = [
        {"value": int(t["id"]), "label": str(t["name"])}
        for t in tournaments_raw
        if isinstance(t, dict) and t.get("id") and t.get("name")
    ]

    teams = []
    players = []

    # ✅ Read selected values (IDs)
    selected_tournament = request.form.get('tournament')
    selected_team = request.form.get('team')
    view_type = request.form.get('view_type', 'batting')

    # ✅ Safe int conversion
    try:
        selected_tournament_id = int(selected_tournament) if selected_tournament else None
    except:
        selected_tournament_id = None

    try:
        selected_team_id = int(selected_team) if selected_team else None
    except:
        selected_team_id = None

    # ==========================================================
    # ✅ TEAM DROPDOWN (value=id, label=shortname)
    # ==========================================================
    if selected_tournament_id:
        teams_data = get_teams_by_tournament(selected_tournament_id) or []
        teams = [
            {"value": int(t["id"]), "label": str(t["name"])}
            for t in teams_data
            if isinstance(t, dict) and t.get("id") and t.get("name")
        ]

    # ==========================================================
    # ✅ Resolve team_id -> team_short_name (AL/HYD/etc)
    # ==========================================================
    selected_team_name = None
    if selected_team_id and teams:
        for t in teams:
            if int(t["value"]) == int(selected_team_id):
                selected_team_name = t["label"]   # <-- "AL"
                break

    # ==========================================================
    # ✅ PLAYERS / BOWLERS (pass team_name like before)
    # ==========================================================
    if selected_tournament_id:
        if view_type == "batting":
            if selected_team_name:
                players = get_players_by_team(selected_tournament_id, selected_team_name)
            else:
                players = get_players_by_tournament(selected_tournament_id)

        elif view_type == "bowling":
            if selected_team_name:
                players = get_bowlers_by_team(selected_tournament_id, selected_team_name)
            else:
                players = get_bowlers_by_tournament(selected_tournament_id)

    return render_template(
        'apps/calendar.html',
        tournaments=tournaments,
        teams=teams,
        players=players,
        selected_tournament=selected_tournament_id,
        selected_team=selected_team_id,
        view_type=view_type
    )


@apps.route("/wagonwheel")
def wagonwheel_api():
    """
    Optional fallback endpoint: returns a generated wagon-wheel PNG base64 for a batter,
    using BATTER_DATA[batter_id]['df'] if available. If not available, returns 400.
    This endpoint is used only as a fallback by the template JS if the precomputed base64
    image isn't embedded client-side.
    """
    batter_id = request.args.get("batter_id", "")
    run_type = request.args.get("runType", "all")

    if not batter_id:
        return jsonify({"error": "Missing batter_id"}), 400

    if batter_id not in BATTER_DATA:
        return jsonify({"error": "Batter not found"}), 404

    entry = BATTER_DATA.get(batter_id, {})
    bdf = entry.get("df", None)
    hand = entry.get("hand", "Right")

    if bdf is None:
        return jsonify({"error": "No delivery dataframe for this batter available on server."}), 400

    try:
        run_filter = None if run_type == "all" else int(run_type)
    except Exception:
        run_filter = None

    img_b64 = generate_wagon_wheel(bdf, batter_hand=hand, filter_runs=run_filter)
    return jsonify({"img": img_b64})



import os, shutil, requests
from flask import send_file

import io, zipfile, requests, os
from flask import send_file, request

@apps.route("/download_metric_videos", methods=["POST"])
def download_metric_videos():
    data = request.json
    batter_id = data.get("batter_id")
    bowler_id = data.get("bowler_id")
    metric_raw = data.get("metric")  # incoming string like 'runs', 'balls', 'O', 'W', etc.
    match_id = data.get("match_id")
    inning_id = data.get("inning_id")
    player_name = (data.get("player_name") or "UnknownPlayer").strip().replace(" ", "_")

    # ✅ Batter metrics map
    batter_metric_map = {
        "runs": "R", "R": "R",
        "balls": "B", "B": "B",
        "fours": "4", "4": "4",
        "sixes": "6", "6": "6"
    }

    # ✅ Bowler metrics map
    bowler_metric_map = {
        "overs": "O", "O": "O",
        "runs": "R", "R": "R", "runs_conceded": "R",
        "wickets": "W", "W": "W",
        "dots": "D", "D": "D"
    }

    # ✅ Normalize metric based on player type
    if batter_id:
        metric = batter_metric_map.get(metric_raw, metric_raw)
    elif bowler_id:
        metric = bowler_metric_map.get(metric_raw, metric_raw)
    else:
        return {"error": "No batter_id or bowler_id provided"}, 400

    # ✅ Fetch videos
    video_urls = fetch_metric_videos(
        batter_id=batter_id,
        bowler_id=bowler_id,
        metric=metric,
        match_id=match_id,
        inning_id=inning_id
    )

    if not video_urls:
        return {"error": f"No videos found for metric {metric}"}, 404

    # ✅ Create an in-memory ZIP
    import io, zipfile, requests, os
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for i, url in enumerate(video_urls, start=1):
            try:
                file_name = f"{player_name}/{metric}/ball{i}.mp4"
                if url.startswith("http"):
                    r = requests.get(url, timeout=15)
                    if r.status_code == 200:
                        zipf.writestr(file_name, r.content)
                else:
                    if os.path.exists(url):
                        with open(url, "rb") as f:
                            zipf.writestr(file_name, f.read())
            except Exception as e:
                print(f"⚠️ Failed to add {url}: {e}")

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        as_attachment=True,
        download_name=f"{player_name}_{metric}.zip",
        mimetype="application/zip"
    )




import subprocess
import json
import os

@apps.route("/browse-folder")
def browse_folder():
    try:
        # Run the select_folder.py script
        result = subprocess.run(
            ["python", os.path.join(os.path.dirname(__file__), "select_folder.py")],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            data = json.loads(result.stdout.strip())
            return jsonify(data)
        else:
            return jsonify({"folder": None, "error": result.stderr}), 500
    except Exception as e:
        return jsonify({"folder": None, "error": str(e)}), 500

@apps.route("/set-video-path", methods=["POST"])
def set_video_path():
    try:
        conn = get_connection()
        data = request.get_json()
        folder = data.get("folder")

        if not folder:
            return jsonify({"error": "No folder provided"}), 400

        # ✅ Save to DB (MySQL syntax)
        cursor = conn.cursor()
        cursor.execute("UPDATE tblSettingsMaster SET setM_VideoPath = %s", (folder,))
        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Video path updated to: {folder}")
        return jsonify({"success": True, "folder": folder})

    except Exception as e:
        print("❌ Error in set_video_path:", e)
        return jsonify({"error": str(e)}), 500


from flask import send_file

@apps.route("/video-file/<path:filename>")
def serve_video(filename):
    import os, urllib.parse
    filename = urllib.parse.unquote(filename)  # decode %20 etc.

    # If it's already an absolute Windows path (D:/ or C:\), just use it
    if os.path.isabs(filename) or ":" in filename:
        file_path = filename
    else:
        parent_path = get_parent_video_path_from_db()
        file_path = os.path.join(parent_path, filename)

    print("🔍 Serving file:", file_path)

    if os.path.exists(file_path):
        return send_file(file_path, mimetype="video/mp4")
    else:
        return f"❌ File not found: {file_path}", 404



@apps.route('/favicon.ico')
def favicon():
    from flask import send_from_directory
    return send_from_directory(os.path.join(apps.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


# # ---------------- Set Remote Instance ----------------
# @apps.route("/set-instance-settings", methods=["POST"])
# def set_instance_settings():
#     data = request.get_json(force=True)

#     # Update the shared dict in place
#     db_override["instance"] = data.get("instance")
#     db_override["ip"] = data.get("ip")
#     db_override["username"] = data.get("username")
#     db_override["password"] = data.get("password")

#     print("✅ Remote DB override set:", db_override)
#     return jsonify({"status": "ok", "message": "Remote DB settings applied!"})


# # ---------------- Clear to Local ----------------
# @apps.route("/clear-instance-settings", methods=["POST"])
# def clear_instance_settings():
#     # Reset all keys instead of reassigning dict
#     db_override["instance"] = None
#     db_override["ip"] = None
#     db_override["username"] = None
#     db_override["password"] = None

#     print("✅ Remote DB override cleared → fallback to local standalone DB.")
#     return jsonify({"status": "ok", "message": "Now using local standalone DB!"})


# ---------------------------------------
# 1. Mailbox Filters (Tournament/Team/Match Dropdowns)
# ---------------------------------------
@apps.route('/apps/apps-mailbox', methods=['GET', 'POST'])
@login_required
def mailbox_filters():
    # 🆕 Filter tournaments by logged-in user's association
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    # ✅ tournaments must return list of dicts like:
    # [{"id":"918","name":"ISPL 2026 (Season 3)"}]
    tournaments = get_all_tournaments(association_id) or []

    # ✅ works for GET + POST
    selected_tournament = request.values.get("tournament")  # tournament_id
    selected_team = request.values.get("team")              # team_id
    selected_match = request.values.get("match")            # match_id

    # ✅ Clean IDs
    selected_tournament = str(selected_tournament).strip() if selected_tournament else None
    selected_team = str(selected_team).strip() if selected_team else None
    selected_match = str(selected_match).strip() if selected_match else None

    # ✅ load teams + matches based on selected IDs
    teams = get_teams_by_tournament(selected_tournament) if selected_tournament else []
    matches = get_matches_by_team(selected_team, selected_tournament) if (selected_team and selected_tournament) else []

    # ✅ No match selected -> show filter page
    if not selected_match:
        return render_template(
            "apps/apps-mailbox.html",
            tournaments=tournaments,
            teams=teams,
            matches=matches,
            selected_tournament=selected_tournament,
            selected_team=selected_team,
            selected_match=None,
            match_header=None
        )

    # ✅ Match selected -> redirect to match-center with IDs
    return redirect(
        url_for(
            "apps.match_center",
            match=selected_match,              # ✅ MATCH_ID
            tournament=selected_tournament,    # ✅ TOURNAMENT_ID
            team=selected_team                 # ✅ TEAM_ID
        )
    )



# ---------------------------------------
# 2. Match Center (Uses JSON Cache Builder)
# ---------------------------------------
@apps.route('/apps/match-center', methods=['GET', 'POST'])
@login_required
def match_center(
    match_id=None,
    tournaments=None,
    teams=None,
    matches=None,
    selected_tournament=None,
    selected_team=None
):
    # ✅ Always pull from request first
    match_id = (
        request.values.get("match")
        or request.values.get("selected_match")
        or match_id
    )
    match_id = str(match_id).strip() if match_id else None

    view_type = request.values.get("view_type", "scorecard")

    selected_tournament = selected_tournament or request.values.get("tournament")
    selected_team = selected_team or request.values.get("team")

    # 🆕 Filter tournaments by logged-in user's association
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    tournaments = tournaments or get_all_tournaments(association_id)
    teams = teams or (get_teams_by_tournament(selected_tournament) if selected_tournament else [])
    matches = matches or (get_matches_by_team(selected_team, selected_tournament) if selected_team else [])

    # ✅ If REPORT selected -> redirect
    if view_type == "report":
        return redirect(url_for(
            "apps.match_reports",
            tournament=selected_tournament,
            team=selected_team,
            match=match_id,
            view_type="report"
        ))

    # ✅ No match selected → only filters
    if not match_id:
        return render_template(
            "apps/apps-mailbox.html",
            tournaments=tournaments,
            teams=teams,
            matches=matches,
            selected_tournament=selected_tournament,
            selected_team=selected_team,
            selected_match=None,
            match_header=None,
            match_innings=[],
            last_12_balls=[],
            scorecard_data={},
            selected_inning="1",
            MatchStatus="",
            view_type="scorecard"
        )

    # ✅ Generate scorecard JSON (match_id only)
    force = request.args.get("force") == "1"
    scorecard_json = generate_scorecard_json(match_id, live=True, force=force) or {}

    match_header = scorecard_json.get("match_header", {}) or {}

    # ✅ DB innings summary
    match_innings = get_match_innings(match_id) or []

    # ✅ Last 12 balls
    last_12_balls = scorecard_json.get("last_12_balls", []) or []
    if not last_12_balls:
        try:
            last_12_balls = get_last_12_deliveries(match_id) or []
        except Exception as e:
            print("❌ last_12_balls fallback error:", e)
            last_12_balls = []

    # ✅ Match Status
    match_status = scorecard_json.get("MatchStatus", "Match Result Not Available")
    match_header["match_status"] = match_status

    # ✅ ==========================
    # ✅ SCORECARD DATA (SAFE BUILD)
    # ✅ ==========================

    # ✅ Try using scorecard_data directly (if exists)
    scorecard_data = scorecard_json.get("scorecard_data", {}) or {}

    # ✅ If scorecard_data missing -> build from innings list
    if not scorecard_data:
        scorecard_data = {}
        for inn in (scorecard_json.get("innings", []) or []):
            inn_no = str(inn.get("inn_no") or inn.get("inning_no") or "")
            if not inn_no:
                continue

            # ✅ your HTML expects "meta", "batters", "bowlers", etc.
            scorecard_data[inn_no] = {
                "meta": inn.get("meta", {}) or {},
                "batters": inn.get("batters", []) or [],
                "bowlers": inn.get("bowlers", []) or [],
                "fall_of_wickets": inn.get("fall_of_wickets", []) or [],
                "partnership_chart": inn.get("partnership_chart", None),
            }

    # ✅ Inject TeamShortName from DB into meta (for tab label)
    try:
        inn_team_map = {str(x.get("Inn_Inning")): (x.get("TeamShortName") or "") for x in match_innings}

        for inn_no, payload in scorecard_data.items():
            if payload is None:
                payload = {}
                scorecard_data[inn_no] = payload

            if "meta" not in payload or payload["meta"] is None:
                payload["meta"] = {}

            if not payload["meta"].get("TeamShortName"):
                payload["meta"]["TeamShortName"] = inn_team_map.get(str(inn_no), "")
    except Exception as e:
        print("⚠️ TeamShortName injection failed:", e)

    # ✅ Default inning selection
    selected_inning = str(next(iter(scorecard_data.keys()), "1"))

    # ✅ Player-name safe keys (right click play/download)
    try:
        for inn_obj in scorecard_json.get("innings", []) or []:
            for batter in inn_obj.get("batters", []) or []:
                raw_name = batter.get("name", "UnknownPlayer")
                batter["player_name"] = raw_name.strip().replace(" ", "_")

                bid = str(batter.get("id", "") or "")
                if bid and bid != "-1" and bid not in BATTER_DATA:
                    BATTER_DATA[bid] = {"df": None, "hand": batter.get("hand", "Right")}

            for bowler in inn_obj.get("bowlers", []) or []:
                raw_name = bowler.get("name", "UnknownPlayer")
                bowler["player_name"] = raw_name.strip().replace(" ", "_")

    except Exception as e:
        print("⚠️ Error preparing batters/bowlers:", e)

    return render_template(
        "apps/apps-mailbox.html",
        tournaments=tournaments,
        teams=teams,
        matches=matches,
        selected_tournament=selected_tournament,
        selected_team=selected_team,
        selected_match=match_id,
        match_header=match_header,
        match_innings=match_innings,
        last_12_balls=last_12_balls,
        scorecard_data=scorecard_data,
        selected_inning=selected_inning,
        MatchStatus=match_status,
        view_type="scorecard"
    )






# ---------------------------------------
# 3. API Endpoint → Get Scorecard JSON
# ---------------------------------------
@apps.route('/apps/scorecard/json/<string:match_id>')
def scorecard_json_api(match_id):
    """
    Return raw JSON data for the given match_id.
    ✅ match_id comes from dropdown / URL params.
    """

    force = request.args.get("force") == "1"

    match_id = str(match_id).strip()

    # ✅ Convert match_id -> match_name
    try:
        conn = get_connection()
        row = pd.read_sql(
            """
            SELECT mchM_MatchName
            FROM tblmatchmaster
            WHERE mchM_Id = %s
            LIMIT 1
            """,
            conn,
            params=(match_id,)
        )
        conn.close()

        match_name = None
        if not row.empty:
            match_name = str(row.iloc[0]["mchM_MatchName"]).strip()

    except Exception as e:
        print("❌ Error fetching match_name from match_id:", e)
        match_name = None

    if not match_name:
        return jsonify({"error": "Invalid match_id / match not found"}), 404

    # ✅ Build JSON using match_name (your existing engine uses match_name)
    data = generate_scorecard_json(match_name, live=True, force=force)

    # ✅ Attach match_status into header safely
    if "match_header" not in data or not isinstance(data["match_header"], dict):
        data["match_header"] = {}

    data["match_header"]["match_status"] = data.get("MatchStatus", "Match Result Not Available")

    return jsonify(data)


@apps.route("/db-test")
def db_test():
    conn = get_connection()
    if not conn:
        return "❌ Could not connect to DB"

    try:
        df = pd.read_sql("SELECT COUNT(*) as rows FROM tblscoremaster", conn)
        return f"✅ Connected, tblscoremaster has {df.iloc[0]['rows']} rows"
    except Exception as e:
        return f"❌ Query failed: {e}"
    
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask import Flask, redirect, url_for, render_template, request, flash

app = Flask(__name__)
app.secret_key = "supersecret"  # change this to a secure key

# Setup Login Manager
login_manager = LoginManager()
login_manager.login_view = "pages.login"   # default page
login_manager.init_app(app)

# Simple User Model
class User(UserMixin):
    def __init__(self, id, username, password):
        self.id = id
        self.username = username
        self.password = password

# Dummy Users (replace with DB later)
users = {
    "admin": User(1, "admin", "admin123"),
    "test": User(2, "test", "test123")
}

@login_manager.user_loader
def load_user(user_id):
    for user in users.values():
        if str(user.id) == str(user_id):
            return user
    return None

# @apps.route("/account/login", methods=["GET", "POST"])
# def login():
#     if request.method == "POST":
#         username = request.form.get("username")
#         password = request.form.get("password")

#         user = users.get(username)
#         if user and user.password == password:
#             login_user(user)
#             flash("Login successful!", "success")
#             return redirect(url_for("apps.dynamic_template_apps_view", template_name="apps-chat"))
#         else:
#             flash("Invalid credentials!", "danger")

#     return render_template("pages/account/login.html")


@apps.route("/account/logout")
def logout():
    logout_user()
    flash("You have been logged out.", "info")
    return redirect(url_for("pages.login"))

from flask_login import current_user
from flask import redirect, url_for

@apps.route("/")
def index():
    if current_user.is_authenticated:
        # Default inside page changed to apps-chat
        return redirect(url_for("apps.advanced_filters"))
    return redirect(url_for("pages.login"))

# --- Multiday Dashboard blank shell (opens empty dashboard but with safe header) ---
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
import pandas as pd

# --- Blank (initial load) ---
@apps.route("/apps/multiday-dashboard", methods=["GET"])
@login_required
def multiday_dashboard_blank():
    """
    Blank load — user selects tournament/team/match.
    Provides default values so the template never breaks.
    """
    tournament = request.args.get("tournament")
    team = request.args.get("team")
    match = request.args.get("match")

    print(f"📊 Opening Multi-day Dashboard for: Tourn={tournament}, Team={team}, Match={match}")

    match_header = {
        "MatchName": match or "Select a Match",
        "DaySessionText": "—",
        "GroundName": "—",
        "match_status": "Awaiting Data"
    }

    # ✅ Provide both `match` and `selected_match` for template safety
    return render_template(
        "apps/multiday-dashboard.html",
        tournament=tournament,
        team=team,
        match=match,
        selected_match=match,
        match_header=match_header
    )


# --- View (renders the charts + data placeholders) ---
@apps.route("/apps/multiday-dashboard/view", methods=["GET"])
@login_required
def multiday_dashboard_view():
    """
    Serves the full dashboard view (HTML), while data is fetched via JSON route.
    """
    match_name = (
        request.values.get("match")
        or request.values.get("selected_match")
        or request.args.get("match")
    )

    if not match_name:
        match_header = {
            "MatchName": "No Match Selected",
            "DaySessionText": "—",
            "GroundName": "—",
            "match_status": "No Data"
        }
        return render_template(
            "apps/multiday-dashboard.html",
            match_header=match_header,
            selected_match=None
        )

    force = request.args.get("force") == "1"
    sc = generate_scorecard_json(match_name, live=True, force=force)

    header = sc.get("match_header", {})
    header["match_status"] = sc.get("MatchStatus", header.get("ResultText", ""))

    return render_template(
        "apps/multiday-dashboard.html",
        selected_match=match_name,
        match_header=header
    )

import os, time, json
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MULTIDAY_DASHBOARD_CACHE_DIR = os.path.join(BASE_DIR, "multiday_dashboard_cache")
os.makedirs(MULTIDAY_DASHBOARD_CACHE_DIR, exist_ok=True)

# --- JSON endpoint used by JS for charts ---
@apps.route("/apps/multiday/json/<string:match_name>", methods=["GET"])
def multiday_dashboard_json(match_name):
    """
    Dashboard JSON endpoint — caches per-innings data in multiday_dashboard_cache like scorecard_cache.
    Auto-refreshes every 1 minute for live matches.
    """
    force = request.args.get("force") == "1"
    cache_prefix = match_name.replace(" ", "_").replace("/", "_")
    cache_files = []

    # ✅ Ensure cache directory exists
    global MULTIDAY_DASHBOARD_CACHE_DIR
    MULTIDAY_DASHBOARD_CACHE_DIR = os.path.join(os.path.dirname(__file__), "multiday_dashboard_cache")
    os.makedirs(MULTIDAY_DASHBOARD_CACHE_DIR, exist_ok=True)

    # --- Read base scorecard (for innings & status)
    sc = generate_scorecard_json(match_name, live=True, force=False)
    header = sc.get("match_header", {})
    match_status = sc.get("MatchStatus", "")
    is_live = "Live" in str(match_status)

    # --- Load valid cached innings if available
    innings_json = []
    cache_valid = True

    for inn_no in range(1, 5):  # up to 4 innings
        cache_path = os.path.join(MULTIDAY_DASHBOARD_CACHE_DIR, f"{cache_prefix}_inn{inn_no}.json")
        cache_files.append(cache_path)

        if os.path.exists(cache_path):
            age = time.time() - os.path.getmtime(cache_path)
            if age < 60 and not force and not is_live:
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        innings_json.append(json.load(f))
                    continue
                except Exception as e:
                    print(f"⚠️ Cache read error {cache_path}: {e}")
            else:
                cache_valid = False
        else:
            cache_valid = False

    # --- If cache invalid or match is live, rebuild
    if not cache_valid or force or is_live:
        print(f"♻️ Rebuilding dashboard JSON for {match_name} (live={is_live}, force={force})")
        rows_df = get_ball_by_ball_data(match_name)

        if rows_df is None or rows_df.empty:
            payload = {"match_header": header, "innings": []}
            payload["match_header"]["match_status"] = match_status
            return jsonify(payload)

        df = pd.DataFrame(rows_df)

        # --- Normalize numeric fields
        num_cols = [
            "scrM_InningNo", "scrM_OverNo", "scrM_DelNo", "scrM_BatsmanRuns",
            "scrM_IsWicket", "scrM_IsWideBall", "scrM_WideRuns", "scrM_IsNoBall",
            "scrM_NoBallRuns", "scrM_DayNo", "scrM_SessionNo"
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        # --- Fill defaults if missing
        if "scrM_DayNo" not in df.columns:
            df["scrM_DayNo"] = 1
        if "scrM_SessionNo" not in df.columns:
            df["scrM_SessionNo"] = pd.cut(
                df.get("scrM_OverNo", 0),
                bins=[-1, 30, 60, 9999],
                labels=[1, 2, 3]
            ).astype(int)

        # --- Derived metrics
        df["runs_total"] = (
            df.get("scrM_BatsmanRuns", 0)
            + df.get("scrM_WideRuns", 0)
            + df.get("scrM_NoBallRuns", 0)
        )
        df["is4"] = (df["scrM_BatsmanRuns"] == 4).astype(int)
        df["is6"] = (df["scrM_BatsmanRuns"] == 6).astype(int)
        df["is0"] = (df["scrM_BatsmanRuns"] == 0).astype(int)

        def worm_chunk(x):
            x = x.sort_values(["scrM_OverNo", "scrM_DelNo"])
            labels = [f"{int(o)}.{int(d)}" for o, d in zip(x["scrM_OverNo"], x["scrM_DelNo"])]
            values = x["runs_total"].cumsum().tolist()
            return {"labels": labels, "values": values}

        # --- Build per-innings JSON
        innings_json = []
        for inn_no, g in df.groupby("scrM_InningNo", sort=True):
            g = g.sort_values(["scrM_DayNo", "scrM_SessionNo", "scrM_OverNo", "scrM_DelNo"])

            per_session = g.groupby(["scrM_DayNo", "scrM_SessionNo"]).agg(
                runs=("runs_total", "sum"),
                wkts=("scrM_IsWicket", "sum"),
                balls=("scrM_DelNo", "count")
            ).reset_index()

            per_day = g.groupby("scrM_DayNo").agg(
                runs=("runs_total", "sum"),
                wkts=("scrM_IsWicket", "sum")
            ).reset_index()

            scoring = g.groupby(["scrM_DayNo", "scrM_SessionNo"]).agg(
                dots=("is0", "sum"),
                ones=("scrM_BatsmanRuns", lambda x: (x == 1).sum()),
                twos=("scrM_BatsmanRuns", lambda x: (x == 2).sum()),
                threes=("scrM_BatsmanRuns", lambda x: (x == 3).sum()),
                fours=("is4", "sum"),
                sixes=("is6", "sum"),
                extras=("scrM_WideRuns", "sum")
            ).reset_index()

            bw = g.groupby(["scrM_DayNo", "scrM_SessionNo", "scrM_PlayMIdBowlerName"]).agg(
                overs=("scrM_OverNo", "count")
            ).reset_index()

            # ✅ FIX: Over Pressure flattened and rr added
            overp = (
                g.groupby(["scrM_DayNo", "scrM_SessionNo", "scrM_OverNo"], as_index=False)
                .agg(runs=("runs_total", "sum"), wkts=("scrM_IsWicket", "sum"))
            )
            overp["rr"] = overp["runs"]
            overp["session_label"] = overp.apply(
                lambda r: f"Day {int(r.scrM_DayNo)}-S{int(r.scrM_SessionNo)}", axis=1
            )
            over_pressure = overp.to_dict(orient="records")

            # --- Current Session Info
            last_row = g.iloc[-1]
            current_session = {
                "Day": int(last_row["scrM_DayNo"]),
                "Session": int(last_row["scrM_SessionNo"]),
                "Innings": int(inn_no),
                "CumulativeRuns": int(g["runs_total"].sum()),
                "Wickets": int(g["scrM_IsWicket"].sum())
            }

            # --- Build innings payload
            inn_data = {
                "inn_no": int(inn_no),
                "worm": worm_chunk(g),
                "per_day": per_day.to_dict(orient="records"),
                "per_session": per_session.to_dict(orient="records"),
                "score_split": scoring.to_dict(orient="records"),
                "bowling_workloads": bw.to_dict(orient="records"),
                "over_pressure": over_pressure,   # ✅ fixed
                "current_session": current_session
            }

            # Save cache file
            inn_path = os.path.join(MULTIDAY_DASHBOARD_CACHE_DIR, f"{cache_prefix}_inn{inn_no}.json")
            with open(inn_path, "w", encoding="utf-8") as f:
                json.dump(inn_data, f, indent=2, ensure_ascii=False)

            innings_json.append(inn_data)

    # --- Final combined payload
    payload = {
        "match_header": header,
        "innings": innings_json
    }
    payload["match_header"]["match_status"] = match_status
    payload["match_header"]["current_session_info"] = [
        inn["current_session"] for inn in innings_json if "current_session" in inn
    ]

    return jsonify(payload)

# ================================
# ✅ GET TOURNAMENT FORMAT ROUTE
# ================================
@apps.route("/apps/get-tournament-format/<string:tournament_id>", methods=["GET"])
def get_tournament_format(tournament_id):
    """
    Returns the match format (e.g., 'T20', 'One Day', 'Multi Day', 'Test') 
    for a given tournament.
    Used by frontend JS to decide whether to open the multiday or limited dashboard.
    """
    try:
        # ✅ Use same DB function as other parts of app (MySQL-safe)
        sql = """
            SELECT z.z_Name AS format
            FROM tbltournaments t
            JOIN tblz z ON t.trnM_MatchFormat_z = z.z_Id
            WHERE t.trnM_Id = %s
        """

        # ✅ get_data_from_db supports (sql, params)
        result = get_data_from_db(sql, (tournament_id,))

        if result and len(result) > 0:
            fmt = result[0]["format"]
            print(f"🎯 Tournament {tournament_id} format detected: {fmt}")
            return jsonify({"format": fmt})
        else:
            print(f"⚠️ No format found for tournament {tournament_id}")
            return jsonify({"format": None}), 404

    except Exception as e:
        print(f"❌ Error fetching tournament format: {e}")
        return jsonify({"format": None}), 500


# ================================
# ✅ LIMITED OVERS DASHBOARD PLACEHOLDER
# ================================
@apps.route("/apps/limited-dashboard/view", methods=["GET"])
@login_required
def limited_dashboard_view():
    """
    Limited Overs Dashboard View — shows real data charts for T20/ODI matches.
    Uses same structure as multiday dashboard but based on limited-overs phases.
    """
    tournament = request.args.get("tournament")
    team = request.args.get("team")
    match = request.args.get("match")

    if not match:
        header = {
            "MatchName": "No Match Selected",
            "DaySessionText": "",
            "GroundName": "",
            "match_status": "No Data",
        }
        return render_template(
            "apps/limited-dashboard.html",
            match_header=header,
            tournament=tournament,
            team=team,
            match=match,
        )

    # 🔹 Load scorecard JSON for match header details
    sc = generate_scorecard_json(match, live=True, force=False)
    header = sc.get("match_header", {})
    header["match_status"] = sc.get("MatchStatus", header.get("ResultText", "Completed"))

    print(f"🟢 Opening Limited Overs Dashboard for match={match}")

    return render_template(
        "apps/limited-dashboard.html",
        match_header=header,
        tournament=tournament,
        team=team,
        match=match,
    )


# ===========================================================
# ✅ Tournament Format API Route
# ===========================================================
@apps.route("/apps/api/tournament-format", methods=["GET"])
@login_required
def api_tournament_format():
    """
    Returns the match format (e.g., T20, One Day, Multi Day, Test) for a given tournament.
    Used by the frontend (Match Center JS) to decide which dashboard to open.
    """
    tournament_id = request.args.get("tournament")
    if not tournament_id:
        return jsonify({"error": "Missing tournament id"}), 400

    try:
        format_name = get_match_format_by_tournament(tournament_id)
        print(f"🎯 Tournament {tournament_id} format = {format_name}")
        if not format_name:
            return jsonify({"format": None}), 404

        return jsonify({"format": format_name}), 200

    except Exception as e:
        print(f"❌ Error fetching tournament format: {e}")
        return jsonify({"format": None, "error": str(e)}), 500


# ===========================================================
# 🟦 LIMITED OVERS DASHBOARD JSON ENDPOINT
# ===========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIMITED_DASHBOARD_CACHE_DIR = os.path.join(BASE_DIR, "limited_dashboard_cache")
os.makedirs(LIMITED_DASHBOARD_CACHE_DIR, exist_ok=True)

@apps.route("/apps/limited-dashboard/json/<string:match_name>", methods=["GET"])
def limited_dashboard_json(match_name):
    """
    JSON endpoint for Limited Overs Dashboard.
    Creates and caches per-innings JSON files inside limited_dashboard_cache,
    similar to multiday_dashboard_cache structure.
    """
    force = request.args.get("force") == "1"
    cache_prefix = match_name.replace(" ", "_").replace("/", "_")

    global LIMITED_DASHBOARD_CACHE_DIR
    LIMITED_DASHBOARD_CACHE_DIR = os.path.join(os.path.dirname(__file__), "limited_dashboard_cache")
    os.makedirs(LIMITED_DASHBOARD_CACHE_DIR, exist_ok=True)

    # --- Read scorecard for header
    sc = generate_scorecard_json(match_name, live=True, force=False)
    header = sc.get("match_header", {})
    match_status = sc.get("MatchStatus", "")
    is_live = "Live" in str(match_status)

    innings_json = []
    cache_valid = True

    # --- Check for 2 innings (T20/ODI)
    for inn_no in range(1, 3):
        cache_path = os.path.join(LIMITED_DASHBOARD_CACHE_DIR, f"{cache_prefix}_inn{inn_no}.json")
        if os.path.exists(cache_path):
            age = time.time() - os.path.getmtime(cache_path)
            if age < 60 and not force and not is_live:
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        innings_json.append(json.load(f))
                    continue
                except Exception as e:
                    print(f"⚠️ Cache read error {cache_path}: {e}")
            else:
                cache_valid = False
        else:
            cache_valid = False

    # --- If cache invalid, rebuild everything
    if not cache_valid or force or is_live:
        print(f"♻️ Rebuilding limited dashboard JSON for {match_name} (live={is_live}, force={force})")

        df = get_ball_by_ball_data(match_name)
        if df is None or df.empty:
            payload = {"match_header": header, "innings": []}
            payload["match_header"]["match_status"] = match_status
            return jsonify(payload)

        df = pd.DataFrame(df)
        num_cols = [
            "scrM_InningNo", "scrM_OverNo", "scrM_DelNo",
            "scrM_BatsmanRuns", "scrM_IsWicket"
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        # Derived metrics
        df["runs_total"] = (
            df.get("scrM_BatsmanRuns", 0)
            + df.get("scrM_WideRuns", 0)
            + df.get("scrM_NoBallRuns", 0)
        )
        df["is4"] = (df["scrM_BatsmanRuns"] == 4).astype(int)
        df["is6"] = (df["scrM_BatsmanRuns"] == 6).astype(int)
        df["is0"] = (df["scrM_BatsmanRuns"] == 0).astype(int)

        # --- Rebuild innings JSONs
        innings_json = []
        for inn_no, g in df.groupby("scrM_InningNo", sort=True):
            g = g.sort_values(["scrM_OverNo", "scrM_DelNo"])

            # 🟦 Worm Chart (Progressive Runs)
            worm = {
                "labels": [f"{int(o)}.{int(d)}" for o, d in zip(g["scrM_OverNo"], g["scrM_DelNo"])],
                "values": g["runs_total"].cumsum().tolist(),
            }

            # 🟩 Overwise summary
            rr = (
                g.groupby("scrM_OverNo")
                .agg(runs=("runs_total", "sum"), wkts=("scrM_IsWicket", "sum"))
                .reset_index()
            )
            rr["rr"] = rr["runs"]

            # 🟨 Phase Summary
            max_over = int(g["scrM_OverNo"].max())

            def phase_label(over):
                if over <= 6:
                    return "Powerplay"
                elif over <= max_over - 5:
                    return "Middle Overs"
                return "Death Overs"

            g["phase"] = g["scrM_OverNo"].apply(phase_label)
            phases = g.groupby("phase").agg(
                runs=("runs_total", "sum"), wkts=("scrM_IsWicket", "sum")
            ).reset_index()

            # 🟧 Boundary Split
            boundary_split = {
                "dots": int(g["is0"].sum()),
                "ones": int((g["scrM_BatsmanRuns"] == 1).sum()),
                "twos": int((g["scrM_BatsmanRuns"] == 2).sum()),
                "threes": int((g["scrM_BatsmanRuns"] == 3).sum()),
                "fours": int(g["is4"].sum()),
                "sixes": int(g["is6"].sum()),
            }

            # 🟪 Bowler Economy
            bowl_econ = (
                g.groupby("scrM_PlayMIdBowlerName")
                .agg(runs=("runs_total", "sum"), balls=("scrM_DelNo", "count"))
                .reset_index()
            )
            bowl_econ["overs"] = (bowl_econ["balls"] / 6).round(1)
            bowl_econ["econ"] = (bowl_econ["runs"] / bowl_econ["overs"]).replace(
                [np.inf, np.nan], 0
            )
            top_bowlers = bowl_econ.sort_values("econ").head(5).to_dict(orient="records")

            # 🟥 Over Pressure (Bubble)
            overp = (
                g.groupby("scrM_OverNo")
                .agg(runs=("runs_total", "sum"), wkts=("scrM_IsWicket", "sum"))
                .reset_index()
            )
            overp["bubble_size"] = overp["wkts"].apply(lambda w: max(4, w * 8))
            overp_data = [
                [int(row.scrM_OverNo), int(row.runs), int(row.bubble_size)]
                for _, row in overp.iterrows()
            ]

            # 🟦 Build per-innings data
            inn_data = {
                "inn_no": int(inn_no),
                "worm": worm,
                "run_rate": rr.to_dict(orient="records"),
                "phase_summary": phases.to_dict(orient="records"),
                "boundary_split": boundary_split,
                "bowler_economy": top_bowlers,
                "over_pressure": overp_data,
            }

            # --- Cache per-innings JSON
            cache_path = os.path.join(
                LIMITED_DASHBOARD_CACHE_DIR, f"{cache_prefix}_inn{inn_no}.json"
            )
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(inn_data, f, indent=2, ensure_ascii=False)

            innings_json.append(inn_data)

    # --- Final combined payload
    payload = {"match_header": header, "innings": innings_json}
    payload["match_header"]["match_status"] = match_status

    return jsonify(payload)


@apps.route('/regenerate_radar_chart', methods=['POST'])
def regenerate_radar_chart():
    """
    API endpoint to regenerate radar chart with filtered data
    """
    from tailwick.utils import generate_radar_chart
    import json
    
    try:
        data = request.get_json()
        player_name = data.get('player_name', 'Player')
        breakdown_data = data.get('breakdown_data', [])
        stance = data.get('stance', 'RHB')
        
        # Generate dummy stats (these won't be displayed, only runs/percentages matter)
        stats = []
        labels = []
        
        # Regenerate the radar chart with filtered breakdown data
        radar_img = generate_radar_chart(player_name, stats, labels, breakdown_data, stance)
        
        return jsonify({
            'success': True,
            'image': radar_img
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@apps.route("/apps/team-analysis", methods=["GET", "POST"])
def team_analysis():
    # 🆕 Filter tournaments by logged-in user's association
    from flask import session
    from flask_login import current_user

    association_id = None
    if current_user and getattr(current_user, "is_authenticated", False):
        association_id = getattr(current_user, "trnM_AssociationId", None) or session.get("association_id")

    tournaments = get_all_tournaments(association_id)

    # ✅ Team Analysis template expects: [{"value":..,"label":..}]
    # but get_all_tournaments() returns: [{"id":..,"name":..}]
    tournaments = tournaments or []
    tournaments = [
        {"value": int(t["id"]), "label": t["name"]}
        for t in tournaments
        if isinstance(t, dict) and t.get("id") and t.get("name")
    ]


    # ================== GET FILTER VALUES ==================
    if request.method == "POST":
        selected_tournament = request.form.get("tournament")
        selected_team = request.form.get("team")
        selected_matches_raw = request.form.getlist("matches[]")
        view_type = request.form.get("view_type", "batting")  # batting default

        # if All is chosen, keep ONLY "All" in the raw list
        if "All" in selected_matches_raw:
            selected_matches_raw = ["All"]
    else:
        selected_tournament = None
        selected_team = None
        selected_matches_raw = []
        view_type = "batting"

    # ================== TEAM DROPDOWN ==================
    teams = []
    if selected_tournament:
        teams_data = get_teams_by_tournament(selected_tournament)

        # ✅ Team Analysis needs NAME ONLY
        # utils gives [{"id":..,"name":..}] so convert it to ["name1","name2"]
        teams = [t.get("name") for t in teams_data if isinstance(t, dict) and t.get("name")]


    # ================== MATCH LIST ==================
    # ================== TEAM ID RESOLVE (Safe) ==================
    team_id = None
    if selected_team and selected_tournament:
        try:
            teams_data = get_teams_by_tournament(selected_tournament)  # [{"id":..,"name":..}]
            selected_team_clean = str(selected_team).strip().lower()

            for t in teams_data:
                tname = str(t.get("name", "")).strip().lower()
                if tname == selected_team_clean:
                    team_id = t.get("id")
                    break

            print("✅ Team Analysis selected_team:", selected_team, "=> team_id:", team_id)

        except Exception as e:
            print("❌ Team Analysis team_id resolve error:", e)
            team_id = None



    # ================== MATCH LIST ==================
    # ================== MATCH LIST ==================
    matches = []
    if selected_tournament and team_id:
        try:
            import pandas as pd 
            conn = get_connection()

            query = """
                SELECT DISTINCT s.scrM_MatchName
                FROM tblscoremaster s
                INNER JOIN tblmatchmaster m ON s.scrM_MchMId = m.mchM_Id
                WHERE m.mchM_TrnMId = %s
                AND (s.scrM_tmMIdBatting = %s OR s.scrM_tmMIdBowling = %s)
                AND s.scrM_MatchName IS NOT NULL
                AND s.scrM_MatchName <> ''
                ORDER BY s.scrM_MatchName
            """

            dfm = pd.read_sql(query, conn, params=(selected_tournament, team_id, team_id))
            conn.close()

            matches = dfm["scrM_MatchName"].dropna().astype(str).tolist() if not dfm.empty else []

            print("✅ Team Analysis matches found:", len(matches))

        except Exception as e:
            print("❌ Team Analysis match list error:", e)
            matches = []





    # ================== FINAL MATCH LIST FOR STATS ==================
    if request.method == "POST" and selected_tournament and selected_team:
        if "All" in selected_matches_raw:
            # use ALL matches for calculations
            selected_matches = matches.copy()
        else:
            # keep only valid matches
            selected_matches = [m for m in selected_matches_raw if m in matches]
    else:
        selected_matches = []

    # ✅ Convert selected match names to match IDs for report functions
    match_ids = []

    if selected_matches:
        try:
            conn = get_connection()

            placeholders = ",".join(["%s"] * len(selected_matches))
            q = f"""
                SELECT DISTINCT scrM_MchMId
                FROM tblscoremaster
                WHERE scrM_MatchName IN ({placeholders})
            """

            df_ids = pd.read_sql(q, conn, params=tuple(selected_matches))
            conn.close()

            match_ids = df_ids["scrM_MchMId"].dropna().astype(int).tolist()

            print("✅ Team Analysis match_ids:", match_ids)

        except Exception as e:
            print("❌ Team Analysis match_id resolve error:", e)
            match_ids = []


    # Default values
    show_distribution_card = False
    batting_first_count = 0
    batting_second_count = 0
    bowling_first_count = 0
    bowling_second_count = 0

    # --------------------------
    # imports from utils (use updated implementations)
    # --------------------------
    import pandas as pd
    try:
        # adjust import path if your utils module path is different
        from utils import (
            map_bowling_category,
            aggregate_batting_vs_type,
            generate_bowling_vs_pace_spin,
        )
    except Exception:
        # fallback if utils is in a package (e.g., tailwick.utils)
        try:
            from tailwick.utils import (
                map_bowling_category,
                aggregate_batting_vs_type,
                generate_bowling_vs_pace_spin,
            )
        except Exception:
            # If both imports fail, raise a clear error (so you can adjust)
            raise ImportError(
                "Could not import map_bowling_category / aggregate_batting_vs_type / generate_bowling_vs_pace_spin. "
                "Ensure they exist in utils.py or tailwick.utils."
            )

    # --------------------------
    # Minimal local helper for summarizing bowling deliveries (opponents combined)
    # --------------------------
    def summarize_delivery_bowling_df(df: pd.DataFrame):
        """
        Delivery-level summary for a bowling deliveries DataFrame.
        FIXES APPLIED:
        - Dot balls exclude wides & no-balls.
        - Fours/Sixes include no-ball boundary hits.
        - Wides/NoBalls counted correctly now that invalid deliveries are included.
        - Overs computed from legal balls only.
        Returns:
        {overs, runs, wkts, nb, wd, dots, fours, sixes, econ}
        """

        empty = {
            "overs": "0.0",
            "runs": 0,
            "wkts": 0,
            "nb": 0,
            "wd": 0,
            "dots": 0,
            "fours": 0,
            "sixes": 0,
            "econ": 0,
        }

        if df is None or df.empty:
            return empty

        import pandas as pd

        # -------------------------------------------
        # Ensure numeric columns exist and are numeric
        # -------------------------------------------
        numeric_cols = [
            "scrM_BatsmanRuns",
            "scrM_WideRuns",
            "scrM_NoBallRuns",
            "scrM_ByeRuns",
            "scrM_LegByeRuns",
            "scrM_IsWicket",
            "scrM_IsNoBall",
            "scrM_IsWideBall",
        ]

        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        # -------------------------------------------
        # Total RUNS conceded (including all extras)
        # -------------------------------------------
        runs = int(
            df.get("scrM_BatsmanRuns", 0).sum()
            + df.get("scrM_WideRuns", 0).sum()
            + df.get("scrM_NoBallRuns", 0).sum()
            + df.get("scrM_ByeRuns", 0).sum()
            + df.get("scrM_LegByeRuns", 0).sum()
        )

        # -------------------------------------------
        # LEGAL deliveries (overs calculation)
        # -------------------------------------------
        legal_balls = int(
            ((df.get("scrM_IsWideBall", 0) == 0) &
            (df.get("scrM_IsNoBall", 0) == 0)).sum()
        )

        overs = f"{legal_balls // 6}.{legal_balls % 6}" if legal_balls else "0.0"
        econ = round(runs / (legal_balls / 6), 2) if legal_balls else 0

        # -------------------------------------------
        # Wickets, NoBalls, Wides
        # -------------------------------------------
        wkts = int(df.get("scrM_IsWicket", 0).sum())
        nb = int(df.get("scrM_IsNoBall", 0).sum())
        wd = int(df.get("scrM_IsWideBall", 0).sum())

        # -------------------------------------------
        # Dot balls → must EXCLUDE wides & no-balls
        # -------------------------------------------
        dots = int(
            (
                (df.get("scrM_BatsmanRuns", 0) == 0)
                & (df.get("scrM_IsWideBall", 0) == 0)
                & (df.get("scrM_IsNoBall", 0) == 0)
            ).sum()
        )

        # -------------------------------------------
        # 4s / 6s → include boundaries hit on no-ball
        # -------------------------------------------
        fours = int((df.get("scrM_BatsmanRuns", 0) == 4).sum())
        sixes = int((df.get("scrM_BatsmanRuns", 0) == 6).sum())

        # -------------------------------------------
        # FINAL SUMMARY
        # -------------------------------------------
        return {
            "overs": overs,
            "runs": runs,
            "wkts": wkts,
            "nb": nb,
            "wd": wd,
            "dots": dots,
            "fours": fours,
            "sixes": sixes,
            "econ": econ,
        }


    # =================================================================
    #              HELPER: ORDER-WISE STATS (TOP/MID/LOW) - kept intact
    # =================================================================
    def compute_order_stats_for_team(match_list, team_name, mode):
        """
        mode: 'batting' or 'bowling'
        returns order_stats structure same as before
        """
        order_stats = {1: {}, 2: {}}
        if not match_list or not team_name:
            return order_stats

        try:
            conn = get_connection()
            df = get_filtered_score_data(conn, match_list)
            conn.close()
        except Exception as e:
            print("❌ Error loading score data for order-wise:", e)
            return order_stats

        if df is None or df.empty:
            return order_stats

        try:
            if mode == "batting":
                df = df[df["scrM_tmMIdBattingName"] == team_name]
            else:
                df = df[df["scrM_tmMIdBowlingName"] == team_name]
        except Exception as e:
            print("⚠️ Error filtering df by team in compute_order_stats_for_team:", e)
            return order_stats

        if df.empty:
            return order_stats

        # numeric ensuring
        cols_to_num = [
            "scrM_OverNo",
            "scrM_DelNo",
            "scrM_InningNo",
            "scrM_BatsmanRuns",
            "scrM_DelRuns",
            "scrM_IsWicket",
            "scrM_IsBoundry",
            "scrM_IsSixer",
        ]
        for col in cols_to_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # assign batting positions
        df = df.sort_values(
            ["scrM_MatchName", "scrM_InningNo", "scrM_OverNo", "scrM_DelNo"],
            ignore_index=True,
        )

        def _assign_bat_pos(g):
            order_map = {}
            next_pos = 1
            positions = []
            for name in g["scrM_PlayMIdStrikerName"]:
                if name not in order_map:
                    order_map[name] = next_pos
                    next_pos += 1
                positions.append(order_map[name])
            g["bat_pos"] = positions
            return g

        df = df.groupby(["scrM_MatchName", "scrM_InningNo"], group_keys=False).apply(
            _assign_bat_pos
        )

        order_ranges = {
            "Top Order": (1, 3),
            "Middle Order": (4, 7),
            "Lower Order": (8, 10),
        }

        for inning in [1, 2]:
            inn_df = df[df["scrM_InningNo"] == inning]

            for order_label, (lo, hi) in order_ranges.items():
                d = inn_df[(inn_df["bat_pos"] >= lo) & (inn_df["bat_pos"] <= hi)]

                if d.empty:
                    if mode == "batting":
                        order_stats[inning][order_label] = {
                            "avg_runs": 0.0,
                            "sr": 0.0,
                            "Dots": 0,
                            "Wkts": 0,
                            "4s": 0,
                            "6s": 0,
                            "30+": 0,
                            "50+": 0,
                            "100+": 0,
                        }
                    else:
                        order_stats[inning][order_label] = {
                            "avg_runs": 0.0,
                            "econ": 0.0,
                            "Dots": 0,
                            "Wkts": 0,
                            "4s": 0,
                            "6s": 0,
                            "2W+": 0,
                            "3W+": 0,
                            "5W+": 0,
                        }
                    continue

                total_runs_bat = float(d["scrM_BatsmanRuns"].sum())
                total_runs_del = float(d["scrM_DelRuns"].sum())
                balls = int(len(d))
                matches_cnt = int(d["scrM_MatchName"].nunique()) if "scrM_MatchName" in d.columns else 0

                if mode == "batting":
                    avg_runs = round(total_runs_bat / matches_cnt, 2) if matches_cnt > 0 else 0.0
                    sr_val = round((total_runs_bat / balls) * 100.0, 2) if balls > 0 else 0.0
                else:
                    avg_runs = round(total_runs_del / matches_cnt, 2) if matches_cnt > 0 else 0.0
                    econ_val = round((total_runs_del * 6.0) / balls, 2) if balls > 0 else 0.0

                dots = int((d["scrM_DelRuns"] == 0).sum())
                wkts = int((d["scrM_IsWicket"] == 1).sum())
                fours = int((d["scrM_IsBoundry"] == 1).sum())
                sixes = int((d["scrM_IsSixer"] == 1).sum())

                # Calculate averages for all stats
                dots_avg = round(dots / matches_cnt, 2) if matches_cnt > 0 else 0.0
                wkts_avg = round(wkts / matches_cnt, 2) if matches_cnt > 0 else 0.0
                fours_avg = round(fours / matches_cnt, 2) if matches_cnt > 0 else 0.0
                sixes_avg = round(sixes / matches_cnt, 2) if matches_cnt > 0 else 0.0

                if mode == "batting":
                    runs_by_batter = d.groupby("scrM_PlayMIdStrikerName")["scrM_BatsmanRuns"].sum()
                    c30 = int(((runs_by_batter >= 30) & (runs_by_batter < 50)).sum())
                    c50 = int(((runs_by_batter >= 50) & (runs_by_batter < 100)).sum())
                    c100 = int((runs_by_batter >= 100).sum())
                    c30_avg = round(c30 / matches_cnt, 2) if matches_cnt > 0 else 0.0
                    c50_avg = round(c50 / matches_cnt, 2) if matches_cnt > 0 else 0.0
                    c100_avg = round(c100 / matches_cnt, 2) if matches_cnt > 0 else 0.0

                    order_stats[inning][order_label] = {
                        "avg_runs": avg_runs,
                        "sr": sr_val,
                        "Dots": dots_avg,
                        "Wkts": wkts_avg,
                        "4s": fours_avg,
                        "6s": sixes_avg,
                        "30+": c30_avg,
                        "50+": c50_avg,
                        "100+": c100_avg,
                    }
                else:
                    wkts_by_bowler = d.groupby("scrM_PlayMIdBowlerName")["scrM_IsWicket"].sum()
                    c2w = int(((wkts_by_bowler >= 2) & (wkts_by_bowler < 3)).sum())
                    c3w = int(((wkts_by_bowler >= 3) & (wkts_by_bowler < 5)).sum())
                    c5w = int((wkts_by_bowler >= 5).sum())
                    c2w_avg = round(c2w / matches_cnt, 2) if matches_cnt > 0 else 0.0
                    c3w_avg = round(c3w / matches_cnt, 2) if matches_cnt > 0 else 0.0
                    c5w_avg = round(c5w / matches_cnt, 2) if matches_cnt > 0 else 0.0

                    order_stats[inning][order_label] = {
                        "avg_runs": avg_runs,
                        "econ": econ_val,
                        "Dots": dots_avg,
                        "Wkts": wkts_avg,
                        "4s": fours_avg,
                        "6s": sixes_avg,
                        "2W+": c2w_avg,
                        "3W+": c3w_avg,
                        "5W+": c5w_avg,
                    }

        return order_stats

    # ===================================================
    #      PROCESS ONLY IF FILTERS APPLIED
    # ===================================================
    if request.method == "POST" and selected_tournament and selected_team and selected_matches:
        try:
            # Inning Wise split based on radio selection
            # if view_type == "batting":
            #     batting_first_count, batting_second_count = get_team_inning_distribution(
            #         selected_tournament, selected_team, selected_matches
            #     )
            # elif view_type == "bowling":
            #     bf, bs = get_team_inning_distribution(
            #         selected_tournament, selected_team, selected_matches
            #     )
            #     bowling_first_count = bs
            #     bowling_second_count = bf

            show_distribution_card = True

            # Compute ORDER-WISE stats (Top/Middle/Lower) for selected team
            order_stats = compute_order_stats_for_team(match_ids, selected_team, view_type)

            # Compute ORDER-WISE stats (Top/Middle/Lower) for opponents (batting and bowling)
            opp_order_stats = {
                1: {"Top Order": {}, "Middle Order": {}, "Lower Order": {}},
                2: {"Top Order": {}, "Middle Order": {}, "Lower Order": {}}
            }

            if view_type in ["batting", "bowling"]:
                try:
                    conn = get_connection()
                    df = get_filtered_score_data(conn, match_ids)  # ✅ correct (IDs)
                    conn.close()

                    if df is not None and not df.empty:

                        # ✅ Opponent teams list
                        if view_type == "batting":
                            df_opponents = df[df["scrM_tmMIdBattingName"] != selected_team].copy()
                            opponent_teams = df_opponents["scrM_tmMIdBattingName"].dropna().unique().tolist()
                        else:
                            df_opponents = df[df["scrM_tmMIdBowlingName"] != selected_team].copy()
                            opponent_teams = df_opponents["scrM_tmMIdBowlingName"].dropna().unique().tolist()

                        # ✅ Initialize opp_order_stats
                        opp_order_stats = {
                            1: {"Top Order": {}, "Middle Order": {}, "Lower Order": {}},
                            2: {"Top Order": {}, "Middle Order": {}, "Lower Order": {}}
                        }

                        # ✅ Loop opponents and accumulate stats
                        for opponent in opponent_teams:
                            # ✅ FIX: use match_ids (NOT selected_matches)
                            stats = compute_order_stats_for_team(match_ids, opponent, view_type)

                            for inning in [1, 2]:
                                for order_key in ["Top Order", "Middle Order", "Lower Order"]:

                                    if view_type == "batting":
                                        stat_keys = ["avg_runs", "sr", "Dots", "Wkts", "4s", "6s", "30+", "50+", "100+"]
                                    else:
                                        stat_keys = ["avg_runs", "econ", "Dots", "Wkts", "4s", "6s", "2W+", "3W+", "5W+"]

                                    for stat_key in stat_keys:
                                        val = stats.get(inning, {}).get(order_key, {}).get(stat_key, 0)

                                        if stat_key not in opp_order_stats[inning][order_key]:
                                            opp_order_stats[inning][order_key][stat_key] = 0

                                        opp_order_stats[inning][order_key][stat_key] += val

                        # ✅ Average all stats across number of opponents
                        num_opponents = max(len(opponent_teams), 1)

                        for inning in [1, 2]:
                            for order_key in ["Top Order", "Middle Order", "Lower Order"]:
                                if view_type == "batting":
                                    for stat_key in ["avg_runs", "sr", "Dots", "Wkts", "4s", "6s", "30+", "50+", "100+"]:
                                        opp_order_stats[inning][order_key][stat_key] = round(
                                            opp_order_stats[inning][order_key].get(stat_key, 0) / num_opponents, 2
                                        )
                                else:
                                    for stat_key in ["avg_runs", "econ", "Dots", "Wkts", "4s", "6s", "2W+", "3W+", "5W+"]:
                                        opp_order_stats[inning][order_key][stat_key] = round(
                                            opp_order_stats[inning][order_key].get(stat_key, 0) / num_opponents, 2
                                        )

                    else:
                        # No data, fill with zeros
                        for inning in [1, 2]:
                            for order_key in ["Top Order", "Middle Order", "Lower Order"]:
                                if view_type == "batting":
                                    opp_order_stats[inning][order_key] = {
                                        "avg_runs": 0,
                                        "sr": 0,
                                        "Dots": 0,
                                        "Wkts": 0,
                                        "4s": 0,
                                        "6s": 0,
                                        "30+": 0,
                                        "50+": 0,
                                        "100+": 0
                                    }
                                else:
                                    opp_order_stats[inning][order_key] = {
                                        "avg_runs": 0,
                                        "econ": 0,
                                        "Dots": 0,
                                        "Wkts": 0,
                                        "4s": 0,
                                        "6s": 0,
                                        "2W+": 0,
                                        "3W+": 0,
                                        "5W+": 0
                                    }

                except Exception as e:
                    print("Error computing opp_order_stats:", e)


            # ---------- Load full deliveries for selected matches (one df)
            conn = get_connection()
            df_all = get_filtered_score_data(conn, match_ids)
            # =========================
            # INNING WISE MATCH DISTRIBUTION (TEAM)
            # =========================
            batting_first_count = 0
            batting_second_count = 0
            bowling_first_count = 0
            bowling_second_count = 0

            if df_all is not None and not df_all.empty:

                # ---- TEAM BATTING ----
                team_bat = df_all[df_all["scrM_tmMIdBattingName"] == selected_team].copy()

                if not team_bat.empty:
                    team_innings = (
                        team_bat[["scrM_MatchName", "scrM_InningNo"]]
                        .drop_duplicates()
                    )

                    batting_first_count = (
                        team_innings[team_innings["scrM_InningNo"] == 1]["scrM_MatchName"]
                        .nunique()
                    )

                    batting_second_count = (
                        team_innings[team_innings["scrM_InningNo"] == 2]["scrM_MatchName"]
                        .nunique()
                    )

                # ---- TEAM BOWLING ----
                team_bowl = df_all[df_all["scrM_tmMIdBowlingName"] == selected_team].copy()

                if not team_bowl.empty:
                    bowl_innings = (
                        team_bowl[["scrM_MatchName", "scrM_InningNo"]]
                        .drop_duplicates()
                    )

                    bowling_first_count = (
                        bowl_innings[bowl_innings["scrM_InningNo"] == 1]["scrM_MatchName"]
                        .nunique()
                    )

                    bowling_second_count = (
                        bowl_innings[bowl_innings["scrM_InningNo"] == 2]["scrM_MatchName"]
                        .nunique()
                    )

            show_distribution_card = True

            conn.close()


            team_radar_b64 = None
            opponent_radar_b64 = None
            opponent_name = "All Opponents"

            # Read run-filter checkboxes
            team1_filters = request.form.getlist("t1_runs") if request.method == "POST" else []
            team2_filters = request.form.getlist("t2_runs") if request.method == "POST" else []

            # Default = All
            if not team1_filters:
                team1_filters = ["all"]
            if not team2_filters:
                team2_filters = ["all"]

            run_filter_team1 = None if "all" in team1_filters else [int(x) for x in team1_filters]
            run_filter_team2 = None if "all" in team2_filters else [int(x) for x in team2_filters]

            if df_all is not None and not df_all.empty:

                mode = "batting" if view_type == "batting" else "bowling"

                # -----------------------------
                #   LEFT — Selected Team Data
                # -----------------------------
                if mode == "batting":
                    df_team_final = df_all[df_all["scrM_tmMIdBattingName"] == selected_team].copy()
                else:
                    df_team_final = df_all[df_all["scrM_tmMIdBowlingName"] == selected_team].copy()

                # ==========================================================
                # NEW FIX 1: keep only valid wagon sectors
                # ==========================================================
                valid_sectors = [
                    "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
                    "Point", "Covers", "Long Off", "Long On"
                ]
                df_team_final = df_team_final[df_team_final["scrM_WagonArea_zName"].isin(valid_sectors)]

                # ==========================================================
                # NEW FIX 2: Normalize runs column
                # ==========================================================
                df_team_final["scrM_BatsmanRuns"] = pd.to_numeric(
                    df_team_final["scrM_BatsmanRuns"], errors="coerce"
                ).fillna(0).astype(int)

                # ==========================================================
                # NEW FIX 3: Proper run filtering
                # ==========================================================
                if run_filter_team1 is not None:
                    df_team_final = df_team_final[df_team_final["scrM_BatsmanRuns"].isin(run_filter_team1)]

                # Generate left radar
                try:
                    team_radar_b64 = generate_team_wagon_radar(
                        selected_team, df_team_final, mode=mode, run_filter=run_filter_team1
                    )

                except Exception as e:
                    print("TEAM RADAR ERROR:", e)
                    team_radar_b64 = None

                # -----------------------------
                #   RIGHT — Opponents Combined
                # -----------------------------
                if mode == "batting":
                    df_opponents_final = df_all[df_all["scrM_tmMIdBattingName"] != selected_team].copy()
                else:
                    df_opponents_final = df_all[df_all["scrM_tmMIdBowlingName"] != selected_team].copy()

                # Valid sectors only
                df_opponents_final = df_opponents_final[df_opponents_final["scrM_WagonArea_zName"].isin(valid_sectors)]

                # Normalize runs
                df_opponents_final["scrM_BatsmanRuns"] = pd.to_numeric(
                    df_opponents_final["scrM_BatsmanRuns"], errors="coerce"
                ).fillna(0).astype(int)

                # Apply run filter
                if run_filter_team2 is not None:
                    df_opponents_final = df_opponents_final[df_opponents_final["scrM_BatsmanRuns"].isin(run_filter_team2)]


                # Generate right radar
                try:
                    opponent_radar_b64 = generate_team_wagon_radar(
                        opponent_name, df_opponents_final, mode=mode, run_filter=run_filter_team2
                    )

                except Exception as e:
                    print("OPPONENT RADAR ERROR:", e)
                    opponent_radar_b64 = None

            # ==========================================
            #    NEW: GRAPHS TAB → RUNS PER OVER LOGIC
            # ==========================================

            runs_per_over_chart = None

            if df_all is not None and not df_all.empty:
                area_col = "scrM_WagonArea_zName"

                valid_areas = [
                    "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
                    "Point", "Covers", "Long Off", "Long On"
                ]

                # ----------------- TEAM BATTING -----------------
                team_batting_df = df_all[
                    (df_all["scrM_tmMIdBattingName"] == selected_team) &
                    (df_all[area_col].isin(valid_areas))
                ]

                # ----------------- OPPONENT BATTING -----------------
                oppo_batting_df = df_all[
                    (df_all["scrM_tmMIdBowlingName"] == selected_team) &
                    (df_all[area_col].isin(valid_areas))
                ]

                team_area_batting = compute_area_stats(team_batting_df, area_col)
                oppo_area_batting = compute_area_stats(oppo_batting_df, area_col)

                # ----------------- TEAM BOWLING -----------------
                team_bowling_df = df_all[
                    (df_all["scrM_tmMIdBowlingName"] == selected_team) &
                    (df_all[area_col].isin(valid_areas))
                ]

                # ----------------- OPPONENT BOWLING -----------------
                oppo_bowling_df = df_all[
                    (df_all["scrM_tmMIdBattingName"] == selected_team) &
                    (df_all[area_col].isin(valid_areas))
                ]

                team_area_bowling = compute_area_stats(team_bowling_df, area_col)
                oppo_area_bowling = compute_area_stats(oppo_bowling_df, area_col)


                # TEAM BATTING / OPPONENTS BATTING
                df_team_bat = df_all[df_all["scrM_tmMIdBattingName"] == selected_team].copy()
                df_oppo_bat = df_all[df_all["scrM_tmMIdBattingName"] != selected_team].copy()

                # TEAM BOWLING / OPPONENTS BOWLING
                df_team_bowl = df_all[df_all["scrM_tmMIdBowlingName"] == selected_team].copy()
                df_oppo_bowl = df_all[df_all["scrM_tmMIdBowlingName"] != selected_team].copy()

                # -----------------------
                #    SWITCH ON RADIO
                # -----------------------
                if view_type == "batting":
                    df_team_final = df_team_bat
                    df_oppo_final = df_oppo_bat
                else:
                    df_team_final = df_team_bowl
                    df_oppo_final = df_oppo_bowl

                # -----------------------
                #    CALL YOUR FUNCTION
                # -----------------------
                chart_obj = create_team_vs_opponent_runs_per_over_chart(
                    df_team_final,
                    df_oppo_final,
                    selected_team,
                    phase=None,
                    dark_mode=False
                )

                runs_per_over_chart = chart_obj.to_html(
                    full_html=False,
                    include_plotlyjs='cdn',
                    config={
                        "responsive": True,
                        "displaylogo": False,
                        "scrollZoom": False,
                    }
                )

                rr_obj = create_team_vs_opponent_run_rate_chart(
                    df_team_final,
                    df_oppo_final,
                    selected_team,
                    phase=None,
                    dark_mode=False
                )

                run_rate_chart = rr_obj.to_html(
                    full_html=False,
                    include_plotlyjs='cdn',
                    config={
                        "responsive": True,
                        "displaylogo": False,
                        "scrollZoom": False,
                    }
                )



            if df_all is None:
                df_all = pd.DataFrame()

            # bowling skill column name
            skill_col = "scrM_BowlerSkill"

            # ---------- Build Team1 and Opponents combined subsets ----------
            # Team1 batting deliveries (selected team batting)
            df_team1_batting = df_all[df_all.get("scrM_tmMIdBattingName") == selected_team].copy()
            # Opponents batting deliveries (batting team != selected_team)
            df_opponents_batting = df_all[df_all.get("scrM_tmMIdBattingName") != selected_team].copy()

            # Team1 bowling deliveries (selected team bowling)
            df_team1_bowling = df_all[df_all.get("scrM_tmMIdBowlingName") == selected_team].copy()
            # Opponents bowling deliveries (bowling team != selected_team)
            df_opponents_bowling = df_all[df_all.get("scrM_tmMIdBowlingName") != selected_team].copy()

            # ---------- Add BowlingType column by mapping skill_col (using utils mapper) ----------
            for _df in [df_team1_batting, df_opponents_batting, df_team1_bowling, df_opponents_bowling]:
                if skill_col in _df.columns:
                    _df["BowlingType"] = _df[skill_col].apply(map_bowling_category)
                else:
                    _df["BowlingType"] = "Unknown"

            # ---------- BATTING VIEW: Pace & Spin batting metrics ----------
            team1_pace_batting_df = df_team1_batting[df_team1_batting["BowlingType"] == "Pace"]
            team1_spin_batting_df = df_team1_batting[df_team1_batting["BowlingType"] == "Spin"]

            opponents_pace_batting_df = df_opponents_batting[df_opponents_batting["BowlingType"] == "Pace"]
            opponents_spin_batting_df = df_opponents_batting[df_opponents_batting["BowlingType"] == "Spin"]

            pace_metrics_team1 = aggregate_batting_vs_type(team1_pace_batting_df)
            spin_metrics_team1 = aggregate_batting_vs_type(team1_spin_batting_df)

            pace_metrics_opponents = aggregate_batting_vs_type(opponents_pace_batting_df)
            spin_metrics_opponents = aggregate_batting_vs_type(opponents_spin_batting_df)

            # ---------- BOWLING VIEW: Pace & Spin bowling metrics ----------
            # Get match count for averaging
            match_count = len(selected_matches) if selected_matches else 1
            

            # For selected team, use the utils generate_bowling_vs_pace_spin (works per team)
            pace_bowl_team1, spin_bowl_team1 = generate_bowling_vs_pace_spin(df_all, selected_team)
            # Calculate averages for all stats (except econ, which is already an average)
            for key in ["overs", "wd", "nb", "dots", "wkts", "fours", "sixes"]:
                if key in pace_bowl_team1:
                    try:
                        pace_bowl_team1[key] = round(float(pace_bowl_team1.get(key, 0)) / max(match_count, 1), 2)
                    except Exception:
                        pace_bowl_team1[key] = 0
                if key in spin_bowl_team1:
                    try:
                        spin_bowl_team1[key] = round(float(spin_bowl_team1.get(key, 0)) / max(match_count, 1), 2)
                    except Exception:
                        spin_bowl_team1[key] = 0
            # Calculate average runs conceded
            pace_bowl_team1["runs"] = round(pace_bowl_team1.get("runs", 0) / max(match_count, 1), 2)
            spin_bowl_team1["runs"] = round(spin_bowl_team1.get("runs", 0) / max(match_count, 1), 2)

            # For opponents combined (multiple teams), summarize delivery-level dfs directly
            pace_bowl_opponents = summarize_delivery_bowling_df(df_opponents_bowling[df_opponents_bowling["BowlingType"] == "Pace"])
            spin_bowl_opponents = summarize_delivery_bowling_df(df_opponents_bowling[df_opponents_bowling["BowlingType"] == "Spin"])
            for key in ["overs", "wd", "nb", "dots", "wkts", "fours", "sixes"]:
                if key in pace_bowl_opponents:
                    try:
                        pace_bowl_opponents[key] = round(float(pace_bowl_opponents.get(key, 0)) / max(match_count, 1), 2)
                    except Exception:
                        pace_bowl_opponents[key] = 0
                if key in spin_bowl_opponents:
                    try:
                        spin_bowl_opponents[key] = round(float(spin_bowl_opponents.get(key, 0)) / max(match_count, 1), 2)
                    except Exception:
                        spin_bowl_opponents[key] = 0
            # Calculate average runs conceded
            pace_bowl_opponents["runs"] = round(pace_bowl_opponents.get("runs", 0) / max(match_count, 1), 2)
            spin_bowl_opponents["runs"] = round(spin_bowl_opponents.get("runs", 0) / max(match_count, 1), 2)

            # ----------------------------------------------------
            #                 BATTING MODE
            # ----------------------------------------------------
            if view_type == "batting":
                pp_stats = get_powerplay_stats(
                    selected_tournament, selected_team, selected_matches
                ) or {}

                pp1 = pp_stats.get("pp1", {})
                pp2 = pp_stats.get("pp2", {})
                mo1 = pp_stats.get("mo1", {})
                mo2 = pp_stats.get("mo2", {})
                so1 = pp_stats.get("so1", {})
                so2 = pp_stats.get("so2", {})


                # --- Opponent Inning Wise Match Distribution ---
                opp_batting_first_count = 0
                opp_batting_second_count = 0
                if df_all is not None and not df_all.empty:
                    # Opponent = all teams except selected_team
                    opp_bat = df_all[df_all["scrM_tmMIdBattingName"] != selected_team].copy()
                    if not opp_bat.empty:
                        # For each match, get the first and second innings batting team (excluding selected_team)
                        match_innings = opp_bat.groupby(["scrM_MatchName", "scrM_InningNo"]).first().reset_index()
                        opp_batting_first_count = (match_innings["scrM_InningNo"] == 1).sum()
                        opp_batting_second_count = (match_innings["scrM_InningNo"] == 2).sum()
                else:
                    opp_bat = None

                # --- Opponent Powerplay phase stats (overs 1-6) ---
                def get_opponent_phase_stats(df, phase_overs, inning_no):
                    # df: all opponent batting deliveries
                    df_inn = df[df["scrM_InningNo"] == inning_no]
                    df_phase = df_inn[(df_inn["scrM_OverNo"] >= phase_overs[0]) & (df_inn["scrM_OverNo"] <= phase_overs[1])]
                    if df_phase.empty:
                        return {"avg_runs": 0, "sr": 0, "dots": 0, "wkts": 0, "fours": 0, "sixes": 0, "matches_cnt": 1}
                    total_runs = df_phase["scrM_BatsmanRuns"].sum()
                    balls = len(df_phase)
                    matches_cnt = df_phase["scrM_MatchName"].nunique() if "scrM_MatchName" in df_phase.columns else 1
                    avg_runs = round(total_runs / matches_cnt, 2) if matches_cnt > 0 else 0
                    sr = round((total_runs / balls) * 100, 2) if balls > 0 else 0
                    dots = (df_phase["scrM_BatsmanRuns"] == 0).sum()
                    wkts = (df_phase["scrM_IsWicket"] == 1).sum()
                    fours = (df_phase["scrM_BatsmanRuns"] == 4).sum()
                    sixes = (df_phase["scrM_BatsmanRuns"] == 6).sum()
                    return {"avg_runs": avg_runs, "sr": sr, "dots": dots, "wkts": wkts, "fours": fours, "sixes": sixes, "matches_cnt": matches_cnt}

                # Powerplay: 1-6, Middle: 7-15, Slog: 16-20
                opp_pp1 = get_opponent_phase_stats(opp_bat, (1, 6), 1) if opp_bat is not None else {}
                opp_pp2 = get_opponent_phase_stats(opp_bat, (1, 6), 2) if opp_bat is not None else {}
                opp_mo1 = get_opponent_phase_stats(opp_bat, (7, 15), 1) if opp_bat is not None else {}
                opp_mo2 = get_opponent_phase_stats(opp_bat, (7, 15), 2) if opp_bat is not None else {}
                opp_so1 = get_opponent_phase_stats(opp_bat, (16, 20), 1) if opp_bat is not None else {}
                opp_so2 = get_opponent_phase_stats(opp_bat, (16, 20), 2) if opp_bat is not None else {}

                return render_template(
                    "apps/team-analysis.html",
                    tournaments=tournaments,
                    teams=teams,
                    matches=matches,
                    selected_tournament=selected_tournament,
                    selected_team=selected_team,
                    selected_matches_raw=selected_matches_raw,
                    selected_matches=selected_matches,
                    view_type=view_type,
                    show_distribution_card=show_distribution_card,
                    batting_first_count=batting_first_count,
                    batting_second_count=batting_second_count,

                    runs_per_over_chart=runs_per_over_chart,
                    run_rate_chart=run_rate_chart,
                    team_radar_b64=team_radar_b64,
                    opponent_radar_b64=opponent_radar_b64,
                    opponent_name=opponent_name,
                    team_area_batting=team_area_batting,
                    oppo_area_batting=oppo_area_batting,

                    # BATTING UI VALUES - PHASE WISE (existing)
                    pp1_avg_runs=pp1.get("avg_runs"),
                    pp1_sr=pp1.get("sr"),
                    pp1_dots=round(pp1.get("dots", 0), 2) if pp1 else 0,
                    pp1_wkts=round(pp1.get("wkts", 0), 2) if pp1 else 0,
                    pp1_fours=round(pp1.get("fours", 0), 2) if pp1 else 0,
                    pp1_sixes=round(pp1.get("sixes", 0), 2) if pp1 else 0,

                    pp2_avg_runs=pp2.get("avg_runs"),
                    pp2_sr=pp2.get("sr"),
                    pp2_dots=pp2.get("dots"),
                    pp2_wkts=pp2.get("wkts"),
                    pp2_fours=pp2.get("fours"),
                    pp2_sixes=pp2.get("sixes"),

                    mo1_avg_runs=mo1.get("avg_runs"),
                    mo1_sr=mo1.get("sr"),
                    mo1_dots=mo1.get("dots"),
                    mo1_wkts=mo1.get("wkts"),
                    mo1_fours=mo1.get("fours"),
                    mo1_sixes=mo1.get("sixes"),

                    mo2_avg_runs=mo2.get("avg_runs"),
                    mo2_sr=mo2.get("sr"),
                    mo2_dots=mo2.get("dots"),
                    mo2_wkts=mo2.get("wkts"),
                    mo2_fours=mo2.get("fours"),
                    mo2_sixes=mo2.get("sixes"),

                    so1_avg_runs=so1.get("avg_runs"),
                    so1_sr=so1.get("sr"),
                    so1_dots=so1.get("dots"),
                    so1_wkts=so1.get("wkts"),
                    so1_fours=so1.get("fours"),
                    so1_sixes=so1.get("sixes"),

                    so2_avg_runs=so2.get("avg_runs"),
                    so2_sr=so2.get("sr"),
                    so2_dots=so2.get("dots"),
                    so2_wkts=so2.get("wkts"),
                    so2_fours=so2.get("fours"),
                    so2_sixes=so2.get("sixes"),

                    # OPPONENT INNING WISE MATCH DISTRIBUTION
                    opp_batting_first_count=opp_batting_first_count,
                    opp_batting_second_count=opp_batting_second_count,

                    # OPPONENT PHASE-WISE STATS (Powerplay, Middle & Slog overs, Batting 1st/2nd)
                    opp_pp1_avg_runs=opp_pp1.get("avg_runs"),
                    opp_pp1_sr=opp_pp1.get("sr"),
                    # Use the same calculation as the modal popup for dots (average per match)
                    opp_pp1_dots=round((opp_pp1.get("dots", 0) / opp_pp1.get("matches_cnt", 1)), 2) if opp_pp1 else 0,
                    opp_pp1_wkts=round((opp_pp1.get("wkts", 0) / opp_pp1.get("matches_cnt", 1)), 2) if opp_pp1 else 0,
                    opp_pp1_fours=round((opp_pp1.get("fours", 0) / opp_pp1.get("matches_cnt", 1)), 2) if opp_pp1 else 0,
                    opp_pp1_sixes=round((opp_pp1.get("sixes", 0) / opp_pp1.get("matches_cnt", 1)), 2) if opp_pp1 else 0,

                    opp_pp2_avg_runs=opp_pp2.get("avg_runs"),
                    opp_pp2_sr=opp_pp2.get("sr"),
                    opp_pp2_dots=round((opp_pp2.get("dots", 0) / opp_pp2.get("matches_cnt", 1)), 2) if opp_pp2 else 0,
                    opp_pp2_wkts=round((opp_pp2.get("wkts", 0) / opp_pp2.get("matches_cnt", 1)), 2) if opp_pp2 else 0,
                    opp_pp2_fours=round((opp_pp2.get("fours", 0) / opp_pp2.get("matches_cnt", 1)), 2) if opp_pp2 else 0,
                    opp_pp2_sixes=round((opp_pp2.get("sixes", 0) / opp_pp2.get("matches_cnt", 1)), 2) if opp_pp2 else 0,

                    opp_mo1_avg_runs=opp_mo1.get("avg_runs"),
                    opp_mo1_sr=opp_mo1.get("sr"),
                    opp_mo1_dots=round((opp_mo1.get("dots", 0) / opp_mo1.get("matches_cnt", 1)), 2) if opp_mo1 else 0,
                    opp_mo1_wkts=round((opp_mo1.get("wkts", 0) / opp_mo1.get("matches_cnt", 1)), 2) if opp_mo1 else 0,
                    opp_mo1_fours=round((opp_mo1.get("fours", 0) / opp_mo1.get("matches_cnt", 1)), 2) if opp_mo1 else 0,
                    opp_mo1_sixes=round((opp_mo1.get("sixes", 0) / opp_mo1.get("matches_cnt", 1)), 2) if opp_mo1 else 0,

                    opp_mo2_avg_runs=opp_mo2.get("avg_runs"),
                    opp_mo2_sr=opp_mo2.get("sr"),
                    opp_mo2_dots=round((opp_mo2.get("dots", 0) / opp_mo2.get("matches_cnt", 1)), 2) if opp_mo2 else 0,
                    opp_mo2_wkts=round((opp_mo2.get("wkts", 0) / opp_mo2.get("matches_cnt", 1)), 2) if opp_mo2 else 0,
                    opp_mo2_fours=round((opp_mo2.get("fours", 0) / opp_mo2.get("matches_cnt", 1)), 2) if opp_mo2 else 0,
                    opp_mo2_sixes=round((opp_mo2.get("sixes", 0) / opp_mo2.get("matches_cnt", 1)), 2) if opp_mo2 else 0,

                    opp_so1_avg_runs=opp_so1.get("avg_runs"),
                    opp_so1_sr=opp_so1.get("sr"),
                    opp_so1_dots=round((opp_so1.get("dots", 0) / opp_so1.get("matches_cnt", 1)), 2) if opp_so1 else 0,
                    opp_so1_wkts=round((opp_so1.get("wkts", 0) / opp_so1.get("matches_cnt", 1)), 2) if opp_so1 else 0,
                    opp_so1_fours=round((opp_so1.get("fours", 0) / opp_so1.get("matches_cnt", 1)), 2) if opp_so1 else 0,
                    opp_so1_sixes=round((opp_so1.get("sixes", 0) / opp_so1.get("matches_cnt", 1)), 2) if opp_so1 else 0,

                    opp_so2_avg_runs=opp_so2.get("avg_runs"),
                    opp_so2_sr=opp_so2.get("sr"),
                    opp_so2_dots=round((opp_so2.get("dots", 0) / opp_so2.get("matches_cnt", 1)), 2) if opp_so2 else 0,
                    opp_so2_wkts=round((opp_so2.get("wkts", 0) / opp_so2.get("matches_cnt", 1)), 2) if opp_so2 else 0,
                    opp_so2_fours=round((opp_so2.get("fours", 0) / opp_so2.get("matches_cnt", 1)), 2) if opp_so2 else 0,
                    opp_so2_sixes=round((opp_so2.get("sixes", 0) / opp_so2.get("matches_cnt", 1)), 2) if opp_so2 else 0,

                    # ORDER-WISE STATS
                    order_stats=order_stats,
                    opp_order_stats=opp_order_stats,

                    # ========== NEW: PACE & SPIN BATTING METRICS ========== 
                    # Pace (left = selected team batting vs pace; right = opponents batting vs pace)
                    pace_team1_avg_runs=pace_metrics_team1.get("avg_runs"),
                    pace_team1_sr=pace_metrics_team1.get("sr"),
                    pace_team1_dots=round(pace_metrics_team1.get("dots", 0) / max(pace_metrics_team1.get("matches", 1), 1), 2) if pace_metrics_team1.get("matches", 0) > 0 else 0,
                    pace_team1_wkts=round(pace_metrics_team1.get("wkts", 0) / max(pace_metrics_team1.get("matches", 1), 1), 2) if pace_metrics_team1.get("matches", 0) > 0 else 0,
                    pace_team1_fours=round(pace_metrics_team1.get("fours", 0) / max(pace_metrics_team1.get("matches", 1), 1), 2) if pace_metrics_team1.get("matches", 0) > 0 else 0,
                    pace_team1_sixes=round(pace_metrics_team1.get("sixes", 0) / max(pace_metrics_team1.get("matches", 1), 1), 2) if pace_metrics_team1.get("matches", 0) > 0 else 0,
                    pace_team1_matches=pace_metrics_team1.get("matches"),

                    pace_oppo_avg_runs=pace_metrics_opponents.get("avg_runs"),
                    pace_oppo_sr=pace_metrics_opponents.get("sr"),
                    pace_oppo_dots=round(pace_metrics_opponents.get("dots", 0) / max(pace_metrics_opponents.get("matches", 1), 1), 2) if pace_metrics_opponents.get("matches", 0) > 0 else 0,
                    pace_oppo_wkts=round(pace_metrics_opponents.get("wkts", 0) / max(pace_metrics_opponents.get("matches", 1), 1), 2) if pace_metrics_opponents.get("matches", 0) > 0 else 0,
                    pace_oppo_fours=round(pace_metrics_opponents.get("fours", 0) / max(pace_metrics_opponents.get("matches", 1), 1), 2) if pace_metrics_opponents.get("matches", 0) > 0 else 0,
                    pace_oppo_sixes=round(pace_metrics_opponents.get("sixes", 0) / max(pace_metrics_opponents.get("matches", 1), 1), 2) if pace_metrics_opponents.get("matches", 0) > 0 else 0,
                    pace_oppo_matches=pace_metrics_opponents.get("matches"),

                    # Spin (left = selected team batting vs spin; right = opponents batting vs spin)
                    spin_team1_avg_runs=spin_metrics_team1.get("avg_runs"),
                    spin_team1_sr=spin_metrics_team1.get("sr"),
                    spin_team1_dots=round(spin_metrics_team1.get("dots", 0) / max(spin_metrics_team1.get("matches", 1), 1), 2) if spin_metrics_team1.get("matches", 0) > 0 else 0,
                    spin_team1_wkts=round(spin_metrics_team1.get("wkts", 0) / max(spin_metrics_team1.get("matches", 1), 1), 2) if spin_metrics_team1.get("matches", 0) > 0 else 0,
                    spin_team1_fours=round(spin_metrics_team1.get("fours", 0) / max(spin_metrics_team1.get("matches", 1), 1), 2) if spin_metrics_team1.get("matches", 0) > 0 else 0,
                    spin_team1_sixes=round(spin_metrics_team1.get("sixes", 0) / max(spin_metrics_team1.get("matches", 1), 1), 2) if spin_metrics_team1.get("matches", 0) > 0 else 0,
                    spin_team1_matches=spin_metrics_team1.get("matches"),

                    spin_oppo_avg_runs=spin_metrics_opponents.get("avg_runs"),
                    spin_oppo_sr=spin_metrics_opponents.get("sr"),
                    spin_oppo_dots=round(spin_metrics_opponents.get("dots", 0) / max(spin_metrics_opponents.get("matches", 1), 1), 2) if spin_metrics_opponents.get("matches", 0) > 0 else 0,
                    spin_oppo_wkts=round(spin_metrics_opponents.get("wkts", 0) / max(spin_metrics_opponents.get("matches", 1), 1), 2) if spin_metrics_opponents.get("matches", 0) > 0 else 0,
                    spin_oppo_fours=round(spin_metrics_opponents.get("fours", 0) / max(spin_metrics_opponents.get("matches", 1), 1), 2) if spin_metrics_opponents.get("matches", 0) > 0 else 0,
                    spin_oppo_sixes=round(spin_metrics_opponents.get("sixes", 0) / max(spin_metrics_opponents.get("matches", 1), 1), 2) if spin_metrics_opponents.get("matches", 0) > 0 else 0,
                    spin_oppo_matches=spin_metrics_opponents.get("matches"),
                )

            # ----------------------------------------------------
            #                 BOWLING MODE
            # ----------------------------------------------------

            if view_type == "bowling":
                bowl = get_phase_stats_bowling(
                    selected_tournament, selected_team, selected_matches
                ) or {}

                # --- Opponent Inning Wise Match Distribution (Bowling) ---
                opp_bowling_first_count = 0
                opp_bowling_second_count = 0
                opp_bowl = None
                if 'df_all' in locals() and df_all is not None and not df_all.empty:
                    opp_bowl = df_all[df_all["scrM_tmMIdBowlingName"] != selected_team].copy()
                    if not opp_bowl.empty:
                        match_innings = opp_bowl.groupby(["scrM_MatchName", "scrM_InningNo"]).first().reset_index()
                        opp_bowling_first_count = (match_innings["scrM_InningNo"] == 1).sum()
                        opp_bowling_second_count = (match_innings["scrM_InningNo"] == 2).sum()

                def get_opponent_bowling_phase_stats(df, phase_overs, inning_no):
                    df_inn = df[df["scrM_InningNo"] == inning_no]
                    df_phase = df_inn[(df_inn["scrM_OverNo"] >= phase_overs[0]) & (df_inn["scrM_OverNo"] <= phase_overs[1])]
                    if df_phase.empty:
                        return {"avg_runs": 0, "econ": 0, "dots": 0, "wkts": 0, "fours": 0, "sixes": 0}
                    total_runs = df_phase["scrM_DelRuns"].sum()
                    balls = len(df_phase)
                    matches_cnt = df_phase["scrM_MatchName"].nunique() if "scrM_MatchName" in df_phase.columns else 1
                    avg_runs = round(total_runs / matches_cnt, 2) if matches_cnt > 0 else 0
                    econ = round((total_runs / balls) * 6, 2) if balls > 0 else 0
                    dots = round(((df_phase["scrM_BatsmanRuns"] == 0) & (df_phase["scrM_IsWideBall"] == 0) & (df_phase["scrM_IsNoBall"] == 0)).sum() / matches_cnt, 2) if matches_cnt > 0 else 0
                    wkts = round(df_phase["scrM_IsWicket"].sum() / matches_cnt, 2) if matches_cnt > 0 else 0
                    fours = round((df_phase["scrM_BatsmanRuns"] == 4).sum() / matches_cnt, 2) if matches_cnt > 0 else 0
                    sixes = round((df_phase["scrM_BatsmanRuns"] == 6).sum() / matches_cnt, 2) if matches_cnt > 0 else 0
                    return {"avg_runs": avg_runs, "econ": econ, "dots": dots, "wkts": wkts, "fours": fours, "sixes": sixes}

                opp_bowl_pp1 = get_opponent_bowling_phase_stats(opp_bowl, (1, 6), 1) if opp_bowl is not None else {}
                opp_bowl_pp2 = get_opponent_bowling_phase_stats(opp_bowl, (1, 6), 2) if opp_bowl is not None else {}
                opp_bowl_mo1 = get_opponent_bowling_phase_stats(opp_bowl, (7, 15), 1) if opp_bowl is not None else {}
                opp_bowl_mo2 = get_opponent_bowling_phase_stats(opp_bowl, (7, 15), 2) if opp_bowl is not None else {}
                opp_bowl_so1 = get_opponent_bowling_phase_stats(opp_bowl, (16, 20), 1) if opp_bowl is not None else {}
                opp_bowl_so2 = get_opponent_bowling_phase_stats(opp_bowl, (16, 20), 2) if opp_bowl is not None else {}

                return render_template(
                    "apps/team-analysis.html",
                    tournaments=tournaments,
                    teams=teams,
                    matches=matches,
                    selected_tournament=selected_tournament,
                    selected_team=selected_team,
                    selected_matches_raw=selected_matches_raw,
                    selected_matches=selected_matches,
                    view_type=view_type,
                    show_distribution_card=show_distribution_card,
                    bowling_first_count=bowling_first_count,
                    bowling_second_count=bowling_second_count,
                    opp_bowling_first_count=opp_bowling_first_count,
                    opp_bowling_second_count=opp_bowling_second_count,
                    runs_per_over_chart=runs_per_over_chart,
                    run_rate_chart=run_rate_chart,
                    team_radar_b64=team_radar_b64,
                    opponent_radar_b64=opponent_radar_b64,
                    opponent_name=opponent_name,
                    team_area_bowling=team_area_bowling,
                    oppo_area_bowling=oppo_area_bowling,

                    # BOWLING – using Avg Runs from util (PHASE WISE)
                    pp1_avg_runs=bowl.get("pp1", {}).get("avg_runs"),
                    pp1_sr=bowl.get("pp1", {}).get("econ"),
                    pp1_dots=bowl.get("pp1", {}).get("dots"),
                    pp1_wkts=bowl.get("pp1", {}).get("wkts"),
                    pp1_fours=bowl.get("pp1", {}).get("fours"),
                    pp1_sixes=bowl.get("pp1", {}).get("sixes"),

                    pp2_avg_runs=bowl.get("pp2", {}).get("avg_runs"),
                    pp2_sr=bowl.get("pp2", {}).get("econ"),
                    pp2_dots=bowl.get("pp2", {}).get("dots"),
                    pp2_wkts=bowl.get("pp2", {}).get("wkts"),
                    pp2_fours=bowl.get("pp2", {}).get("fours"),
                    pp2_sixes=bowl.get("pp2", {}).get("sixes"),

                    mo1_avg_runs=bowl.get("mo1", {}).get("avg_runs"),
                    mo1_sr=bowl.get("mo1", {}).get("econ"),
                    mo1_dots=bowl.get("mo1", {}).get("dots"),
                    mo1_wkts=bowl.get("mo1", {}).get("wkts"),
                    mo1_fours=bowl.get("mo1", {}).get("fours"),
                    mo1_sixes=bowl.get("mo1", {}).get("sixes"),

                    mo2_avg_runs=bowl.get("mo2", {}).get("avg_runs"),
                    mo2_sr=bowl.get("mo2", {}).get("econ"),
                    mo2_dots=bowl.get("mo2", {}).get("dots"),
                    mo2_wkts=bowl.get("mo2", {}).get("wkts"),
                    mo2_fours=bowl.get("mo2", {}).get("fours"),
                    mo2_sixes=bowl.get("mo2", {}).get("sixes"),

                    so1_avg_runs=bowl.get("so1", {}).get("avg_runs"),
                    so1_sr=bowl.get("so1", {}).get("econ"),
                    so1_dots=bowl.get("so1", {}).get("dots"),
                    so1_wkts=bowl.get("so1", {}).get("wkts"),
                    so1_fours=bowl.get("so1", {}).get("fours"),
                    so1_sixes=bowl.get("so1", {}).get("sixes"),

                    so2_avg_runs=bowl.get("so2", {}).get("avg_runs"),
                    so2_sr=bowl.get("so2", {}).get("econ"),
                    so2_dots=bowl.get("so2", {}).get("dots"),
                    so2_wkts=bowl.get("so2", {}).get("wkts"),
                    so2_fours=bowl.get("so2", {}).get("fours"),
                    so2_sixes=bowl.get("so2", {}).get("sixes"),

                    # OPPONENT BOWLING PHASE-WISE STATS (Powerplay, Middle & Slog overs, Bowling 1st/2nd)
                    opp_pp1_avg_runs=opp_bowl_pp1.get("avg_runs"),
                    opp_pp1_sr=opp_bowl_pp1.get("econ"),
                    opp_pp1_dots=round(opp_bowl_pp1.get("dots", 0), 2) if opp_bowl_pp1 else 0,
                    opp_pp1_wkts=round(opp_bowl_pp1.get("wkts", 0), 2) if opp_bowl_pp1 else 0,
                    opp_pp1_fours=round(opp_bowl_pp1.get("fours", 0), 2) if opp_bowl_pp1 else 0,
                    opp_pp1_sixes=round(opp_bowl_pp1.get("sixes", 0), 2) if opp_bowl_pp1 else 0,

                    opp_pp2_avg_runs=opp_bowl_pp2.get("avg_runs"),
                    opp_pp2_sr=opp_bowl_pp2.get("econ"),
                    opp_pp2_dots=round(opp_bowl_pp2.get("dots", 0), 2) if opp_bowl_pp2 else 0,
                    opp_pp2_wkts=round(opp_bowl_pp2.get("wkts", 0), 2) if opp_bowl_pp2 else 0,
                    opp_pp2_fours=round(opp_bowl_pp2.get("fours", 0), 2) if opp_bowl_pp2 else 0,
                    opp_pp2_sixes=round(opp_bowl_pp2.get("sixes", 0), 2) if opp_bowl_pp2 else 0,

                    opp_mo1_avg_runs=opp_bowl_mo1.get("avg_runs"),
                    opp_mo1_sr=opp_bowl_mo1.get("econ"),
                    opp_mo1_dots=opp_bowl_mo1.get("dots"),
                    opp_mo1_wkts=opp_bowl_mo1.get("wkts"),
                    opp_mo1_fours=opp_bowl_mo1.get("fours"),
                    opp_mo1_sixes=opp_bowl_mo1.get("sixes"),

                    opp_mo2_avg_runs=opp_bowl_mo2.get("avg_runs"),
                    opp_mo2_sr=opp_bowl_mo2.get("econ"),
                    opp_mo2_dots=opp_bowl_mo2.get("dots"),
                    opp_mo2_wkts=opp_bowl_mo2.get("wkts"),
                    opp_mo2_fours=opp_bowl_mo2.get("fours"),
                    opp_mo2_sixes=opp_bowl_mo2.get("sixes"),

                    opp_so1_avg_runs=opp_bowl_so1.get("avg_runs"),
                    opp_so1_sr=opp_bowl_so1.get("econ"),
                    opp_so1_dots=opp_bowl_so1.get("dots"),
                    opp_so1_wkts=opp_bowl_so1.get("wkts"),
                    opp_so1_fours=opp_bowl_so1.get("fours"),
                    opp_so1_sixes=opp_bowl_so1.get("sixes"),

                    opp_so2_avg_runs=opp_bowl_so2.get("avg_runs"),
                    opp_so2_sr=opp_bowl_so2.get("econ"),
                    opp_so2_dots=opp_bowl_so2.get("dots"),
                    opp_so2_wkts=opp_bowl_so2.get("wkts"),
                    opp_so2_fours=opp_bowl_so2.get("fours"),
                    opp_so2_sixes=opp_bowl_so2.get("sixes"),

                    # ORDER-WISE STATS
                    order_stats=order_stats,

                    # OPPONENT ORDER-WISE STATS (for bowling)
                    opp_order_stats=opp_order_stats,

                    # ========== NEW: PACE & SPIN BOWLING METRICS ========== 
                    pace_bowl_team1_runs=pace_bowl_team1.get("runs"),
                    pace_bowl_team1_econ=pace_bowl_team1.get("econ"),
                    pace_bowl_team1_overs=pace_bowl_team1.get("overs"),
                    pace_bowl_team1_wd=pace_bowl_team1.get("wd"),
                    pace_bowl_team1_nb=pace_bowl_team1.get("nb"),
                    pace_bowl_team1_dots=pace_bowl_team1.get("dots"),
                    pace_bowl_team1_wkts=pace_bowl_team1.get("wkts"),
                    pace_bowl_team1_fours=pace_bowl_team1.get("fours"),
                    pace_bowl_team1_sixes=pace_bowl_team1.get("sixes"),

                    pace_bowl_oppo_runs=pace_bowl_opponents.get("runs"),
                    pace_bowl_oppo_econ=pace_bowl_opponents.get("econ"),
                    pace_bowl_oppo_overs=pace_bowl_opponents.get("overs"),
                    pace_bowl_oppo_wd=pace_bowl_opponents.get("wd"),
                    pace_bowl_oppo_nb=pace_bowl_opponents.get("nb"),
                    pace_bowl_oppo_dots=pace_bowl_opponents.get("dots"),
                    pace_bowl_oppo_wkts=pace_bowl_opponents.get("wkts"),
                    pace_bowl_oppo_fours=pace_bowl_opponents.get("fours"),
                    pace_bowl_oppo_sixes=pace_bowl_opponents.get("sixes"),

                    spin_bowl_team1_runs=spin_bowl_team1.get("runs"),
                    spin_bowl_team1_econ=spin_bowl_team1.get("econ"),
                    spin_bowl_team1_overs=spin_bowl_team1.get("overs"),
                    spin_bowl_team1_wd=spin_bowl_team1.get("wd"),
                    spin_bowl_team1_nb=spin_bowl_team1.get("nb"),
                    spin_bowl_team1_dots=spin_bowl_team1.get("dots"),
                    spin_bowl_team1_wkts=spin_bowl_team1.get("wkts"),
                    spin_bowl_team1_fours=spin_bowl_team1.get("fours"),
                    spin_bowl_team1_sixes=spin_bowl_team1.get("sixes"),

                    spin_bowl_oppo_runs=spin_bowl_opponents.get("runs"),
                    spin_bowl_oppo_econ=spin_bowl_opponents.get("econ"),
                    spin_bowl_oppo_overs=spin_bowl_opponents.get("overs"),
                    spin_bowl_oppo_wd=spin_bowl_opponents.get("wd"),
                    spin_bowl_oppo_nb=spin_bowl_opponents.get("nb"),
                    spin_bowl_oppo_dots=spin_bowl_opponents.get("dots"),
                    spin_bowl_oppo_wkts=spin_bowl_opponents.get("wkts"),
                    spin_bowl_oppo_fours=spin_bowl_opponents.get("fours"),
                    spin_bowl_oppo_sixes=spin_bowl_opponents.get("sixes"),
                )

        except Exception as e:
            print("❌ ERROR in team_analysis:", e)

    # ================== FIRST LOAD / NO FILTERS ==================
    return render_template(
        "apps/team-analysis.html",
        tournaments=tournaments,
        teams=teams,
        matches=matches,
        selected_tournament=selected_tournament,
        selected_team=selected_team,
        selected_matches_raw=selected_matches_raw,
        selected_matches=selected_matches,
        view_type=view_type,
        show_distribution_card=False,
    )

@apps.route("/apps/api/team_wagon_filter", methods=["POST"])
def api_team_wagon_filter():
    """
    AJAX endpoint for team vs opponents wagon wheel filter.
    """

    import pandas as pd

    data = request.get_json() or {}

    selected_team = data.get("selected_team")
    selected_matches = data.get("selected_matches") or []   # ✅ These are Match NAMES now
    view_type = data.get("view_type", "batting")

    t1_runs = data.get("t1_run_filter", None)
    t2_runs = data.get("t2_run_filter", None)

    # ------------------------------------------------------------
    # Normalize run filters
    # ------------------------------------------------------------
    def normalize_runs(arr):
        """Convert ["1","4"] → [1,4],  ["all"] → None"""
        if arr is None:
            return None

        # If single string
        if isinstance(arr, str):
            if arr.lower() == "all":
                return None
            try:
                return [int(arr)]
            except:
                return None

        # If list-like
        if isinstance(arr, (list, tuple, set)):
            arr = [str(x).strip().lower() for x in arr]

            # If contains ALL → means no filtering
            if "all" in arr:
                return None

            out = []
            for x in arr:
                try:
                    out.append(int(x))
                except:
                    pass

            return out if out else None

        return None

    t1_rf = normalize_runs(t1_runs)
    t2_rf = normalize_runs(t2_runs)

    # ------------------------------------------------------------
    # Convert MatchName -> MatchId (IMPORTANT ✅)
    # ------------------------------------------------------------
    match_ids = []
    try:
        conn = get_connection()

        if selected_matches:
            placeholders = ",".join(["%s"] * len(selected_matches))
            q = f"""
                SELECT DISTINCT scrM_MchMId
                FROM tblscoremaster
                WHERE scrM_MatchName IN ({placeholders})
            """
            df_ids = pd.read_sql(q, conn, params=tuple(selected_matches))
            match_ids = df_ids["scrM_MchMId"].dropna().astype(int).tolist()

        # ------------------------------------------------------------
        # Load ball-by-ball data using match_ids
        # ------------------------------------------------------------
        df_all = get_filtered_score_data(conn, match_ids)
        conn.close()

    except Exception as e:
        print("TEAM WAGON API DB ERROR:", e)
        return jsonify({"error": "DB connection failed"}), 500

    if df_all is None or df_all.empty:
        df_all = pd.DataFrame()

    # ------------------------------------------------------------
    # Select TEAM and OPPONENT DF according to batting/bowling mode
    # ------------------------------------------------------------
    try:
        if view_type == "batting":
            df_team = df_all[df_all["scrM_tmMIdBattingName"] == selected_team].copy()
            df_oppo = df_all[df_all["scrM_tmMIdBattingName"] != selected_team].copy()
        else:
            df_team = df_all[df_all["scrM_tmMIdBowlingName"] == selected_team].copy()
            df_oppo = df_all[df_all["scrM_tmMIdBowlingName"] != selected_team].copy()
    except Exception as e:
        print("TEAM WAGON API TEAM/OPPO SPLIT ERROR:", e)
        df_team = pd.DataFrame()
        df_oppo = pd.DataFrame()

    # ------------------------------------------------------------
    # VALID Wagon sectors
    # ------------------------------------------------------------
    valid_sectors = [
        "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
        "Point", "Covers", "Long Off", "Long On"
    ]

    try:
        df_team = df_team[df_team["scrM_WagonArea_zName"].isin(valid_sectors)]
        df_oppo = df_oppo[df_oppo["scrM_WagonArea_zName"].isin(valid_sectors)]
    except:
        df_team = pd.DataFrame()
        df_oppo = pd.DataFrame()

    # ------------------------------------------------------------
    # Convert runs → numeric
    # ------------------------------------------------------------
    for _df in [df_team, df_oppo]:
        try:
            _df["scrM_BatsmanRuns"] = pd.to_numeric(
                _df["scrM_BatsmanRuns"], errors="coerce"
            ).fillna(0).astype(int)
        except:
            _df["scrM_BatsmanRuns"] = 0

    # ------------------------------------------------------------
    # APPLY RUN FILTER (1s, 2s, 3s, 4s, 6s)
    # ------------------------------------------------------------
    if t1_rf is not None:
        df_team = df_team[df_team["scrM_BatsmanRuns"].isin(t1_rf)]

    if t2_rf is not None:
        df_oppo = df_oppo[df_oppo["scrM_BatsmanRuns"].isin(t2_rf)]

    # ------------------------------------------------------------
    # Generate Team & Opponent Wagon Radar Images
    # ------------------------------------------------------------
    try:
        team_img = generate_team_wagon_radar(
            selected_team or "Team",
            df_team,
            mode=view_type,
            run_filter=t1_rf
        )
    except Exception as e:
        print("TEAM RADAR GENERATION ERROR:", e)
        team_img = None

    try:
        opponent_img = generate_team_wagon_radar(
            "Opponents",
            df_oppo,
            mode=view_type,
            run_filter=t2_rf
        )
    except Exception as e:
        print("OPPONENT RADAR GENERATION ERROR:", e)
        opponent_img = None

    return jsonify({
        "team_img": team_img,
        "opponent_img": opponent_img
    })



@apps.route("/apps/api/player_radar_filter", methods=["POST"])
def api_player_radar_filter():
    """
    API endpoint to filter player radar chart by run types
    Expects JSON: {player_name, filters, selected_team, selected_matches, selected_type}
    Returns: {radar_img: base64_string}
    """
    import pandas as pd

    try:
        from utils import generate_player_radar_chart, get_connection, get_filtered_score_data
    except ImportError:
        from tailwick.utils import generate_player_radar_chart, get_connection, get_filtered_score_data

    try:
        data = request.get_json() or {}
        print("🎯 Player Radar Filter API Called with data:", data)

        player_name = data.get("player_name")
        filters = data.get("filters", ["all"])
        selected_team = data.get("selected_team")

        # ✅ MatchNames coming from frontend
        selected_matches = data.get("selected_matches", [])

        selected_type = data.get("selected_type", "batter")

        if not player_name:
            return jsonify({"error": "Player name is required"}), 400

        # ------------------------------------------------------------
        # Convert MatchName -> MatchId (IMPORTANT ✅)
        # ------------------------------------------------------------
        match_ids = []
        try:
            conn = get_connection()

            if selected_matches:
                placeholders = ",".join(["%s"] * len(selected_matches))
                q = f"""
                    SELECT DISTINCT scrM_MchMId
                    FROM tblscoremaster
                    WHERE scrM_MatchName IN ({placeholders})
                """
                df_ids = pd.read_sql(q, conn, params=tuple(selected_matches))
                match_ids = df_ids["scrM_MchMId"].dropna().astype(int).tolist()

            # ✅ Load data using match_ids
            df_all = get_filtered_score_data(conn, match_ids)
            conn.close()

        except Exception as e:
            print(f"❌ DB Error: {e}")
            return jsonify({"error": "Database query failed"}), 500

        if df_all is None or df_all.empty:
            print("❌ No data returned from database")
            return jsonify({"error": "No data available"}), 404

        # ------------------------------------------------------------
        # Filter by player and team
        # ------------------------------------------------------------
        if selected_type == "batter":
            name_col = "scrM_PlayMIdStrikerName"
            team_col = "scrM_tmMIdBattingName"
        else:
            name_col = "scrM_PlayMIdBowlerName"
            team_col = "scrM_tmMIdBowlingName"

        player_df = df_all[df_all[name_col] == player_name].copy()

        if selected_team and team_col in player_df.columns:
            player_df = player_df[player_df[team_col] == selected_team].copy()

        if player_df.empty:
            print(f"❌ No data found for player: {player_name}")
            return jsonify({"error": "No player data found"}), 404

        print(f"✅ Retrieved player_df with {len(player_df)} rows, selected_team: {selected_team}")

        # Normalize filters - if "all" is included, set run_filter to None
        if "all" in filters:
            run_filter = None
        else:
            run_filter = filters

        print(f"🎨 Generating radar with run_filter: {run_filter}")

        radar_img = generate_player_radar_chart(
            player_df,
            player_name,
            selected_type,
            selected_team,
            run_filter
        )

        if not radar_img:
            return jsonify({"error": "Failed to generate radar chart"}), 500

        print("✅ Radar chart generated successfully")
        return jsonify({"radar_img": radar_img})

    except Exception as e:
        print(f"❌ Player Radar Filter Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



@apps.route("/apps/api/get_pacespin_matchwise_data", methods=["POST"])
def api_get_pacespin_matchwise_data():
    """
    API endpoint to get match-wise pace/spin data for modal popup
    Expects JSON: {tournament, team, matches, ball_type, team_type, view_type}
    Returns: {consolidated: {...}, matchwise: [...]}
    """
    # Import required functions
    try:
        from utils import map_bowling_category
    except ImportError:
        from tailwick.utils import map_bowling_category

    try:
        data = request.get_json()
        print("🎾 Pace/Spin API Called with data:", data)

        tournament = data.get('tournament')
        team = data.get('team')
        matches = data.get('matches', [])
        ball_type = data.get('ball_type')      # 'pace' or 'spin'
        team_type = data.get('team_type')      # 'team' or 'opponent'
        view_type = data.get('view_type')      # 'batting' or 'bowling'

        # -------------------- VALIDATION --------------------
        if not all([tournament, team, ball_type, team_type, view_type]):
            missing = [
                k for k in ['tournament', 'team', 'ball_type', 'team_type', 'view_type']
                if not data.get(k)
            ]
            return jsonify({"error": f"Missing parameters: {', '.join(missing)}"}), 400

        if not matches:
            return jsonify({"error": "Missing parameters - Matches"}), 400

        print(
            f"✅ Parameters validated - Tournament: {tournament}, "
            f"Team: {team}, Matches: {len(matches)}, "
            f"BallType: {ball_type}, TeamType: {team_type}, ViewType: {view_type}"
        )

        # -------------------- DB CONNECTION --------------------
        conn = get_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        try:
            # -------------------- TEAM FILTER LOGIC --------------------
            if view_type == 'batting':
                if team_type == 'team':
                    team_filter = "scrM_tmMIdBattingName"
                    team_value = team
                else:  # opponent batting (we are bowling)
                    team_filter = "scrM_tmMIdBowlingName"
                    team_value = team
            else:  # bowling
                if team_type == 'team':
                    team_filter = "scrM_tmMIdBowlingName"
                    team_value = team
                else:  # opponent bowling (we are batting)
                    team_filter = "scrM_tmMIdBattingName"
                    team_value = team

            # -------------------- SQL QUERY --------------------
            query = f"""
            SELECT
                scrM_MatchName,
                scrM_BatsmanRuns,
                scrM_DelRuns,
                scrM_IsWicket,
                scrM_IsBoundry,
                scrM_IsSixer,
                scrM_IsWideBall,
                scrM_IsNoBall,
                scrM_BowlerSkill
            FROM tblscoremaster
            WHERE scrM_TrnMId = %s
              AND {team_filter} = %s
              AND scrM_MatchName IN %s
            ORDER BY scrM_MatchName, scrM_OverNo, scrM_DelNo
            """

            params = (
                int(tournament),
                team_value,
                tuple(matches)
            )

            print("[DEBUG] SQL Query:", query)
            print("[DEBUG] SQL Params:", params)

            # ✅ EXECUTE SAFELY (SQLAlchemy-compatible)
            df = pd.read_sql(query, conn, params=params)
            print(f"📈 Query returned {len(df)} rows")

            if df.empty:
                return jsonify({
                    "consolidated": {
                        "avg_runs": "0",
                        "sr_or_econ": "0",
                        "dots": "0",
                        "wkts": "0",
                        "fours": "0",
                        "sixes": "0",
                        "overs": "0",
                        "wd": "0",
                        "nb": "0"
                    },
                    "matchwise": []
                })

            # -------------------- BOWLING TYPE FILTER --------------------
            df['BowlingType'] = df['scrM_BowlerSkill'].apply(map_bowling_category)
            target_type = 'Pace' if ball_type == 'pace' else 'Spin'
            df = df[df['BowlingType'] == target_type]

            print(f"📊 After BowlingType={target_type}: {len(df)} rows")

            if df.empty:
                return jsonify({
                    "consolidated": {
                        "avg_runs": "0",
                        "sr_or_econ": "0",
                        "dots": "0",
                        "wkts": "0",
                        "fours": "0",
                        "sixes": "0",
                        "overs": "0",
                        "wd": "0",
                        "nb": "0"
                    },
                    "matchwise": []
                })

            # -------------------- LEGAL BALL FILTER --------------------
            df['is_legal_ball'] = (
                (~df['scrM_IsWideBall'].astype(bool)) &
                (~df['scrM_IsNoBall'].astype(bool))
            )
            legal_df = df[df['is_legal_ball']]

            total_balls = len(legal_df)
            match_count = df['scrM_MatchName'].nunique()
            total_runs = legal_df['scrM_BatsmanRuns'].sum()

            # -------------------- CONSOLIDATED STATS --------------------
            if view_type == 'batting':
                avg_runs = round(total_runs / match_count, 2) if match_count else 0
                sr_or_econ = round((total_runs / total_balls) * 100, 2) if total_balls else 0
            else:
                avg_runs = round(total_runs / match_count, 2) if match_count else 0
                sr_or_econ = round((total_runs * 6) / total_balls, 2) if total_balls else 0

            dots = round((legal_df['scrM_DelRuns'] == 0).sum() / match_count, 2) if match_count else 0
            wkts = round((legal_df['scrM_IsWicket'] == 1).sum() / match_count, 2) if match_count else 0
            fours = round((legal_df['scrM_IsBoundry'] == 1).sum() / match_count, 2) if match_count else 0
            sixes = round((legal_df['scrM_IsSixer'] == 1).sum() / match_count, 2) if match_count else 0

            avg_balls = total_balls / match_count if match_count else 0
            avg_overs = f"{int(avg_balls) // 6}.{int(avg_balls) % 6}"
            avg_wides = round((df['scrM_IsWideBall'] == 1).sum() / match_count, 2) if match_count else 0
            avg_noballs = round((df['scrM_IsNoBall'] == 1).sum() / match_count, 2) if match_count else 0

            consolidated = {
                "avg_runs": str(avg_runs),
                "sr_or_econ": str(sr_or_econ),
                "dots": str(dots),
                "wkts": str(wkts),
                "fours": str(fours),
                "sixes": str(sixes),
                "overs": avg_overs,
                "wd": str(avg_wides),
                "nb": str(avg_noballs)
            }

            # -------------------- MATCHWISE STATS --------------------
            matchwise = []
            for match in matches:
                mdf = df[df['scrM_MatchName'] == match]
                if mdf.empty:
                    continue

                legal = mdf[mdf['is_legal_ball']]
                balls = len(legal)
                runs = legal['scrM_BatsmanRuns'].sum()

                if view_type == 'batting':
                    sr = round((runs / balls) * 100, 2) if balls else 0
                else:
                    sr = round((runs * 6) / balls, 2) if balls else 0

                matchwise.append({
                    "match": match,
                    "avg_runs": str(round(runs, 2)),
                    "sr_or_econ": str(sr),
                    "dots": str((legal['scrM_DelRuns'] == 0).sum()),
                    "wkts": str((legal['scrM_IsWicket'] == 1).sum()),
                    "fours": str((legal['scrM_IsBoundry'] == 1).sum()),
                    "sixes": str((legal['scrM_IsSixer'] == 1).sum()),
                    "overs": f"{balls // 6}.{balls % 6}",
                    "wd": str((mdf['scrM_IsWideBall'] == 1).sum()),
                    "nb": str((mdf['scrM_IsNoBall'] == 1).sum())
                })

            print("✅ Pace/Spin popup data generated successfully")

            return jsonify({
                "consolidated": consolidated,
                "matchwise": matchwise
            })

        finally:
            conn.close()

    except Exception as e:
        import traceback
        print("❌ Error in pace/spin API:", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



@apps.route("/apps/api/get_order_matchwise_data", methods=["POST"])
def api_get_order_matchwise_data():
    """
    API endpoint to get match-wise order data for modal popup
    Expects JSON: {tournament, team, matches, order, inning, view_type}
    Returns: {consolidated: {...}, matchwise: [...]}
    """
    try:
        data = request.get_json()
        print("🎯 Order API Called with data:", data)

        tournament = data.get('tournament')
        team = data.get('team')
        matches = data.get('matches', [])
        order = data.get('order')  # 'top', 'middle', 'lower'
        inning = int(data.get('inning'))
        view_type = data.get('view_type')

        # -------------------- OPPONENT LOGIC --------------------
        is_opponent = view_type in ('batting_opponent', 'bowling_opponent')
        orig_team = team
        orig_view_type = view_type

        consolidated_team = team
        consolidated_view_type = view_type

        if is_opponent and matches:
            match_str = matches[0]
            if ' vs ' in match_str:
                teamA, teamB = match_str.split(' vs ')
                consolidated_team = teamB if teamA == orig_team else teamA

            consolidated_view_type = (
                'batting' if view_type == 'batting_opponent' else 'bowling'
            )

        # -------------------- VALIDATION --------------------
        if not all([tournament, team, order, inning, view_type]):
            missing = [
                k for k in ['tournament', 'team', 'order', 'inning', 'view_type']
                if not data.get(k)
            ]
            return jsonify({"error": f"Missing parameters: {', '.join(missing)}"}), 400

        if not matches:
            return jsonify({"error": "Missing parameters - Matches"}), 400

        print(
            f"✅ Parameters validated - Tournament: {tournament}, "
            f"Team: {team}, Matches: {len(matches)}, "
            f"Order: {order}, Inning: {inning}, ViewType: {view_type}"
        )

        # -------------------- ORDER RANGES --------------------
        order_ranges = {
            "top": (1, 3),
            "middle": (4, 7),
            "lower": (8, 10)
        }

        if order not in order_ranges:
            return jsonify({"error": f"Invalid order: {order}"}), 400

        lo, hi = order_ranges[order]
        print(f"📍 Order range: positions {lo} to {hi}")

        # -------------------- DB CONNECTION --------------------
        conn = get_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        try:
            # Team filter
            team_filter = (
                "scrM_tmMIdBattingName"
                if consolidated_view_type == 'batting'
                else "scrM_tmMIdBowlingName"
            )

            # -------------------- SQL QUERY --------------------
            query = f"""
            WITH BattingPositions AS (
                SELECT
                    scrM_MatchName,
                    scrM_InningNo,
                    scrM_PlayMIdStrikerName,
                    MIN(CAST(scrM_OverNo AS FLOAT) * 6 + CAST(scrM_DelNo AS FLOAT)) AS FirstBall,
                    ROW_NUMBER() OVER (
                        PARTITION BY scrM_MatchName, scrM_InningNo
                        ORDER BY MIN(CAST(scrM_OverNo AS FLOAT) * 6 + CAST(scrM_DelNo AS FLOAT))
                    ) AS BatPos
                FROM tblscoremaster
                WHERE scrM_TrnMId = %s
                  AND {team_filter} = %s
                  AND scrM_MatchName IN %s
                  AND scrM_IsWideBall = 0
                  AND scrM_IsNoBall = 0
                GROUP BY scrM_MatchName, scrM_InningNo, scrM_PlayMIdStrikerName
            )
            SELECT
                s.scrM_MatchName,
                s.scrM_InningNo,
                s.scrM_BatsmanRuns,
                s.scrM_DelRuns,
                s.scrM_IsWicket,
                s.scrM_IsBoundry,
                s.scrM_IsSixer,
                s.scrM_PlayMIdStrikerName,
                s.scrM_PlayMIdBowlerName,
                s.scrM_IsWideBall,
                s.scrM_IsNoBall,
                s.scrM_tmMIdBattingName,
                s.scrM_tmMIdBowlingName,
                bp.BatPos
            FROM tblscoremaster s
            INNER JOIN BattingPositions bp
                ON s.scrM_MatchName = bp.scrM_MatchName
               AND s.scrM_InningNo = bp.scrM_InningNo
               AND s.scrM_PlayMIdStrikerName = bp.scrM_PlayMIdStrikerName
            WHERE s.scrM_TrnMId = %s
              AND {team_filter} = %s
              AND s.scrM_MatchName IN %s
              AND s.scrM_InningNo = %s
              AND bp.BatPos BETWEEN %s AND %s
            ORDER BY s.scrM_MatchName, s.scrM_OverNo, s.scrM_DelNo
            """

            params = (
                int(tournament),
                consolidated_team,
                tuple(matches),

                int(tournament),
                consolidated_team,
                tuple(matches),

                int(inning),
                int(lo),
                int(hi)
            )

            print("[DEBUG] SQL Query:", query)
            print("[DEBUG] SQL Params:", params)

            # ✅ EXECUTE SAFELY (same as Phase-wise)
            df = pd.read_sql(query, conn, params=params)
            print(f"📈 Query returned {len(df)} rows")

            if df.empty:
                return jsonify({
                    "consolidated": {
                        "avg_runs": "0",
                        "sr_or_econ": "0",
                        "dots": "0",
                        "wkts": "0",
                        "fours": "0",
                        "sixes": "0",
                        "milestone1": "0",
                        "milestone2": "0",
                        "milestone3": "0"
                    },
                    "matchwise": []
                })

            # -------------------- DATA PROCESSING --------------------
            df['is_legal_ball'] = (
                (~df['scrM_IsWideBall'].astype(bool)) &
                (~df['scrM_IsNoBall'].astype(bool))
            )

            legal_df = df[df['is_legal_ball']]
            total_balls = len(legal_df)
            match_count = df['scrM_MatchName'].nunique()
            total_runs = legal_df['scrM_BatsmanRuns'].sum()

            if consolidated_view_type == 'batting':
                avg_runs = round(total_runs / match_count, 2) if match_count else 0
                sr_or_econ = round((total_runs / total_balls) * 100, 2) if total_balls else 0
            else:
                avg_runs = round(total_runs / match_count, 2) if match_count else 0
                sr_or_econ = round((total_runs * 6) / total_balls, 2) if total_balls else 0

            consolidated = {
                "avg_runs": str(avg_runs),
                "sr_or_econ": str(sr_or_econ),
                "dots": str((legal_df['scrM_DelRuns'] == 0).sum()),
                "wkts": str((legal_df['scrM_IsWicket'] == 1).sum()),
                "fours": str((legal_df['scrM_IsBoundry'] == 1).sum()),
                "sixes": str((legal_df['scrM_IsSixer'] == 1).sum()),
                "milestone1": "0",
                "milestone2": "0",
                "milestone3": "0"
            }

            # -------------------- MATCHWISE --------------------
            matchwise = []
            for match in matches:
                mdf = df[df['scrM_MatchName'] == match]
                if mdf.empty:
                    continue

                legal = mdf[mdf['is_legal_ball']]
                balls = len(legal)
                runs = legal['scrM_BatsmanRuns'].sum()

                if consolidated_view_type == 'batting':
                    sr = round((runs / balls) * 100, 2) if balls else 0
                else:
                    sr = round((runs * 6) / balls, 2) if balls else 0

                matchwise.append({
                    "match": match,
                    "avg_runs": str(round(runs, 2)),
                    "sr_or_econ": str(sr),
                    "dots": str((legal['scrM_DelRuns'] == 0).sum()),
                    "wkts": str((legal['scrM_IsWicket'] == 1).sum()),
                    "fours": str((legal['scrM_IsBoundry'] == 1).sum()),
                    "sixes": str((legal['scrM_IsSixer'] == 1).sum()),
                    "milestone1": "0",
                    "milestone2": "0",
                    "milestone3": "0"
                })

            print("✅ Order-wise popup data generated successfully")

            return jsonify({
                "consolidated": consolidated,
                "matchwise": matchwise
            })

        finally:
            conn.close()

    except Exception as e:
        import traceback
        print("❌ Error in order API:", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



@apps.route("/apps/api/get_phase_matchwise_data", methods=["POST"])
def api_get_phase_matchwise_data():
    """
    API endpoint to get match-wise phase statistics for the team analysis modal.
    Returns consolidated stats + match-wise breakdown for a specific phase and inning.
    """
    data = request.get_json() or {}
    
    selected_tournament = data.get("tournament")
    selected_team = data.get("team")
    selected_matches = data.get("matches", [])
    phase = data.get("phase")  # 'pp', 'mo', 'so'
    inning = data.get("inning")  # 1 or 2
    view_type = data.get("view_type", "batting")
    # For 'bowling_opponent', treat as opponent bowling filter
    logic_view_type = view_type
    
    print(f"📊 Phase API Called - Tournament: {selected_tournament}, Team: {selected_team}, Matches: {selected_matches}, Phase: {phase}, Inning: {inning}, View: {view_type}")
    
    if not all([selected_tournament, selected_team, selected_matches, phase, inning]):
        error_msg = f"Missing parameters - Tournament: {selected_tournament}, Team: {selected_team}, Matches: {len(selected_matches) if selected_matches else 0}, Phase: {phase}, Inning: {inning}"
        print(f"❌ {error_msg}")
        return jsonify({"error": error_msg}), 400
    
    try:
        conn = get_connection()
        if not conn:
            print("❌ Database connection failed")
            return jsonify({"error": "Database connection failed"}), 500
        # Use a single %s for IN clause and pass matches as a tuple
        placeholders = "%s"
        phase_ranges = {
            'pp': (1, 6),
            'mo': (7, 15),
            'so': (16, 20)
        }
        over_start, over_end = phase_ranges.get(phase, (1, 6))
        if view_type == "batting":
            team_filter = "scrM_tmMIdBattingName"
            team_value = selected_team
        elif view_type == "batting_opponent":
            team_filter = "scrM_tmMIdBowlingName"
            team_value = selected_team
        elif view_type == "bowling_opponent":
            team_filter = "scrM_tmMIdBattingName"
            team_value = selected_team
        else:
            team_filter = "scrM_tmMIdBowlingName"
            team_value = selected_team
        query = f"""
            SELECT 
                scrM_MatchName,
                scrM_InningNo,
                scrM_BatsmanRuns,
                scrM_IsWicket,
                scrM_IsBoundry,
                scrM_IsSixer,
                scrM_OverNo,
                scrM_tmMIdBattingName,
                scrM_tmMIdBowlingName,
                CASE WHEN scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END AS DotBall
            FROM tblscoremaster
            WHERE scrM_TrnMId = %s
              AND {team_filter} = %s
              AND scrM_MatchName IN %s
              AND scrM_IsValidBall = 1
              AND scrM_InningNo = %s
              AND scrM_OverNo >= %s AND scrM_OverNo <= %s
        """
        params = (int(selected_tournament), team_value, tuple(selected_matches), int(inning), over_start, over_end)
        print(f"[DEBUG] SQL Query: {query}")
        print(f"[DEBUG] SQL Params: {params}")
        try:
            df = pd.read_sql(query, conn, params=params)
            print(f"[DEBUG] DataFrame shape: {df.shape}")
        except Exception as sql_e:
            print(f"[ERROR] Exception during pd.read_sql: {sql_e}")
            import traceback
            print(traceback.format_exc())
            return jsonify({"error": str(sql_e)}), 500
        finally:
            conn.close()
        print(f"📈 Query returned {len(df)} rows")

        if df.empty:
            print("⚠️ No data found for the given parameters")
            return jsonify({
                "consolidated": {
                    "avg_runs": 0,
                    "sr_or_econ": "0" if view_type == "batting" else "0%",
                    "sr_or_econ_label": "Strike Rate" if view_type == "batting" else "Economy",
                    "dots": 0,
                    "wkts": 0,
                    "fours": 0,
                    "sixes": 0
                },
                "matchwise": []
            })

        # For 'batting_opponent', calculate batting stats for all teams except selected_team (i.e., all opponents)
        if view_type == "batting_opponent":
            df = df[df["scrM_tmMIdBattingName"] != selected_team]
            total_runs = df['scrM_BatsmanRuns'].sum()
            total_balls = len(df)
            total_dots = df['DotBall'].sum()
            total_wkts = df['scrM_IsWicket'].sum()
            total_fours = df['scrM_IsBoundry'].sum()
            total_sixes = df['scrM_IsSixer'].sum()
            match_count = df['scrM_MatchName'].nunique()
            avg_runs = round(total_runs / match_count, 2) if match_count > 0 else 0
            sr_or_econ = round((total_runs / total_balls) * 100, 2) if total_balls > 0 else 0
            sr_or_econ_label = "Strike Rate"
            consolidated = {
                "avg_runs": avg_runs,
                "sr_or_econ": f"{sr_or_econ}%",
                "sr_or_econ_label": sr_or_econ_label,
                "dots": round(total_dots / match_count, 2) if match_count > 0 else 0,
                "wkts": round(total_wkts / match_count, 2) if match_count > 0 else 0,
                "fours": round(total_fours / match_count, 2) if match_count > 0 else 0,
                "sixes": round(total_sixes / match_count, 2) if match_count > 0 else 0
            }
            matchwise = []
            for match_name in df['scrM_MatchName'].unique():
                match_df = df[df['scrM_MatchName'] == match_name]
                match_runs = match_df['scrM_BatsmanRuns'].sum()
                match_balls = len(match_df)
                match_dots = match_df['DotBall'].sum()
                match_wkts = match_df['scrM_IsWicket'].sum()
                match_fours = match_df['scrM_IsBoundry'].sum()
                match_sixes = match_df['scrM_IsSixer'].sum()
                match_sr_econ = round((match_runs / match_balls) * 100, 2) if match_balls > 0 else 0
                match_sr_econ_str = f"{match_sr_econ}%"
                # Only add if at least one stat is nonzero (actual data)
                if any([match_runs, match_sr_econ, match_dots, match_wkts, match_fours, match_sixes]):
                    matchwise.append({
                        "match": match_name,
                        "avg_runs": int(match_runs),
                        "sr_or_econ": match_sr_econ_str,
                        "dots": int(match_dots),
                        "wkts": int(match_wkts),
                        "fours": int(match_fours),
                        "sixes": int(match_sixes)
                    })
        elif view_type == "bowling_opponent":
            # For opponent bowling, calculate bowling stats for all teams except selected_team (i.e., all opponents)
            if "scrM_tmMIdBowlingName" not in df.columns:
                print("❌ Missing scrM_tmMIdBowlingName column in DataFrame for bowling_opponent!")
                return jsonify({"error": "Missing column in data."}), 500
            df = df[df["scrM_tmMIdBowlingName"] != selected_team]
            if df.empty:
                consolidated = {
                    "avg_runs": 0,
                    "sr_or_econ": "0",
                    "sr_or_econ_label": "Economy",
                    "dots": 0,
                    "wkts": 0,
                    "fours": 0,
                    "sixes": 0
                }
                matchwise = []
            else:
                total_runs = df['scrM_BatsmanRuns'].sum()
                total_balls = len(df)
                total_dots = df['DotBall'].sum()
                total_wkts = df['scrM_IsWicket'].sum()
                total_fours = df['scrM_IsBoundry'].sum()
                total_sixes = df['scrM_IsSixer'].sum()
                match_count = df['scrM_MatchName'].nunique()
                avg_runs = round(total_runs / match_count, 2) if match_count > 0 else 0
                sr_or_econ = round((total_runs / total_balls) * 6, 2) if total_balls > 0 else 0
                sr_or_econ_label = "Economy"
                consolidated = {
                    "avg_runs": avg_runs,
                    "sr_or_econ": str(sr_or_econ),
                    "sr_or_econ_label": sr_or_econ_label,
                    "dots": round(total_dots / match_count, 2) if match_count > 0 else 0,
                    "wkts": round(total_wkts / match_count, 2) if match_count > 0 else 0,
                    "fours": round(total_fours / match_count, 2) if match_count > 0 else 0,
                    "sixes": round(total_sixes / match_count, 2) if match_count > 0 else 0
                }
                matchwise = []
                for match_name in df['scrM_MatchName'].unique():
                    match_df = df[df['scrM_MatchName'] == match_name]
                    match_runs = match_df['scrM_BatsmanRuns'].sum()
                    match_balls = len(match_df)
                    match_dots = match_df['DotBall'].sum()
                    match_wkts = match_df['scrM_IsWicket'].sum()
                    match_fours = match_df['scrM_IsBoundry'].sum()
                    match_sixes = match_df['scrM_IsSixer'].sum()
                    match_sr_econ = round((match_runs / match_balls) * 6, 2) if match_balls > 0 else 0
                    match_sr_econ_str = str(match_sr_econ)
                    # Only add if at least one stat is nonzero (actual data)
                    if any([match_runs, match_sr_econ, match_dots, match_wkts, match_fours, match_sixes]):
                        matchwise.append({
                            "match": match_name,
                            "avg_runs": int(match_runs),
                            "sr_or_econ": match_sr_econ_str,
                            "dots": int(match_dots),
                            "wkts": int(match_wkts),
                            "fours": int(match_fours),
                            "sixes": int(match_sixes)
                        })
        else:
            total_runs = df['scrM_BatsmanRuns'].sum()
            total_balls = len(df)
            total_dots = df['DotBall'].sum()
            total_wkts = df['scrM_IsWicket'].sum()
            total_fours = df['scrM_IsBoundry'].sum()
            total_sixes = df['scrM_IsSixer'].sum()
            match_count = df['scrM_MatchName'].nunique()
            if logic_view_type == "batting":
                avg_runs = round(total_runs / match_count, 2) if match_count > 0 else 0
                sr_or_econ = round((total_runs / total_balls) * 100, 2) if total_balls > 0 else 0
                sr_or_econ_label = "Strike Rate"
            else:
                avg_runs = round(total_runs / match_count, 2) if match_count > 0 else 0
                sr_or_econ = round((total_runs / total_balls) * 6, 2) if total_balls > 0 else 0
                sr_or_econ_label = "Economy"
            consolidated = {
                "avg_runs": avg_runs,
                "sr_or_econ": f"{sr_or_econ}{'%' if logic_view_type == 'batting' else ''}",
                "sr_or_econ_label": sr_or_econ_label,
                "dots": round(total_dots / match_count, 2) if match_count > 0 else 0,
                "wkts": round(total_wkts / match_count, 2) if match_count > 0 else 0,
                "fours": round(total_fours / match_count, 2) if match_count > 0 else 0,
                "sixes": round(total_sixes / match_count, 2) if match_count > 0 else 0
            }
            matchwise = []
            for match_name in df['scrM_MatchName'].unique():
                match_df = df[df['scrM_MatchName'] == match_name]
                match_runs = match_df['scrM_BatsmanRuns'].sum()
                match_balls = len(match_df)
                match_dots = match_df['DotBall'].sum()
                match_wkts = match_df['scrM_IsWicket'].sum()
                match_fours = match_df['scrM_IsBoundry'].sum()
                match_sixes = match_df['scrM_IsSixer'].sum()
                if logic_view_type == "batting":
                    match_sr_econ = round((match_runs / match_balls) * 100, 2) if match_balls > 0 else 0
                    match_sr_econ_str = f"{match_sr_econ}%"
                else:
                    match_sr_econ = round((match_runs / match_balls) * 6, 2) if match_balls > 0 else 0
                    match_sr_econ_str = str(match_sr_econ)
                matchwise.append({
                    "match": match_name,
                    "avg_runs": int(match_runs),
                    "sr_or_econ": match_sr_econ_str,
                    "dots": int(match_dots),
                    "wkts": int(match_wkts),
                    "fours": int(match_fours),
                    "sixes": int(match_sixes)
                })
        
        print(f"✅ Returning data: {match_count} matches, {len(matchwise)} match-wise records")
        
        return jsonify({
            "consolidated": consolidated,
            "matchwise": matchwise
        })
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"❌ ERROR in api_get_phase_matchwise_data: {e}")
        print(f"Stack trace:\n{error_trace}")
        return jsonify({"error": str(e)}), 500


































