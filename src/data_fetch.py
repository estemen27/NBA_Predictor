from nba_api.stats.endpoints import leaguegamelog
import pandas as pd



from nba_api.stats.endpoints import leaguegamelog
import pandas as pd


def get_current_season_team_logs(
    season: str = "2025-26",
    season_type: str = "Regular Season"
) -> pd.DataFrame:
    """
    Descarga el game log de TODOS los equipos para una temporada y tipo de temporada,
    usando LeagueGameLog como fuente.

    Devuelve un dataframe TEAM-GAME (1 fila = 1 equipo en 1 partido).
    """
    logs = leaguegamelog.LeagueGameLog(
        player_or_team_abbreviation="T",      # T = Team, P = Player
        season=season,
        season_type_all_star=season_type     # 'Regular Season', 'Playoffs', etc.
    )

    df = logs.get_data_frames()[0]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df = df.sort_values(["GAME_DATE", "GAME_ID", "TEAM_ID"]).reset_index(drop=True)
    return df





def build_game_level_dataset(df_team_logs: pd.DataFrame,
                             max_games: int | None = None) -> pd.DataFrame:
    """
    A partir de logs por equipo (TEAM-GAME) construye un dataset a nivel partido (GAME),
    con info de local y visitante en una sola fila.

    Si max_games no es None, devuelve solo los primeros max_games partidos
    por orden cronológico.
    """
    df = df_team_logs.copy()

    # Flag local/visitante basado en MATCHUP (" vs. " = local, " @ " = visita)
    df["IS_HOME"] = df["MATCHUP"].str.contains(" vs. ")

    # Separar home y away
    df_home = df[df["IS_HOME"]].copy()
    df_away = df[~df["IS_HOME"]].copy()

    # Renombrar columnas para HOME
    df_home = df_home.rename(columns={
        "TEAM_ID": "HOME_TEAM_ID",
        "TEAM_NAME": "HOME_TEAM_NAME",
        "TEAM_ABBREVIATION": "HOME_TEAM_ABBR",
        "PTS": "HOME_PTS",
    })

    # Renombrar columnas para AWAY
    df_away = df_away.rename(columns={
        "TEAM_ID": "AWAY_TEAM_ID",
        "TEAM_NAME": "AWAY_TEAM_NAME",
        "TEAM_ABBREVIATION": "AWAY_TEAM_ABBR",
        "PTS": "AWAY_PTS",
    })

    # Seleccionar solo columnas necesarias (OJO: sin SEASON_YEAR)
    df_home = df_home[[
        "GAME_ID", "GAME_DATE",
        "HOME_TEAM_ID", "HOME_TEAM_NAME", "HOME_TEAM_ABBR",
        "HOME_PTS"
    ]]

    df_away = df_away[[
        "GAME_ID",
        "AWAY_TEAM_ID", "AWAY_TEAM_NAME", "AWAY_TEAM_ABBR",
        "AWAY_PTS"
    ]]

    # Merge para obtener una fila por partido
    df_games = df_home.merge(df_away, on="GAME_ID", how="inner")

    # Targets básicos
    df_games["MARGIN_HOME"] = df_games["HOME_PTS"] - df_games["AWAY_PTS"]
    df_games["HOME_WIN"] = (df_games["MARGIN_HOME"] > 0).astype(int)

    # Ordenar por fecha y limitar número de partidos
    df_games = df_games.sort_values(["GAME_DATE", "GAME_ID"])
    if max_games is not None:
        df_games = df_games.head(max_games)

    df_games = df_games.reset_index(drop=True)
    return df_games



def get_current_season_games_lite(season: str = "2025-26",
                                  season_type: str = "Regular Season",
                                  max_games: int = 15) -> pd.DataFrame:
    """
    Función de alto nivel:
    - Descarga logs de equipos
    - Construye dataset a nivel partido
    - Limita a los primeros max_games partidos
    """
    df_team_logs = get_current_season_team_logs(season, season_type)
    df_games = build_game_level_dataset(df_team_logs, max_games=max_games)
    return df_games

if __name__ == "__main__":
    df_test = get_current_season_team_logs()
    print(df_test.head())
    print("Partidos descargados:", len(df_test))
