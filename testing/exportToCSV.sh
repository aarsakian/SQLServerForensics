#!/bin/bash
CONTAINER="sql1"

#set -euo pipefail
export $(grep -v '^#' .secrets.env | xargs)


if [[ $# -eq 1 ]]; then
  EXPORT_DIR="$1"
fi
echo $# arguments provided, export directory: $EXPORT_DIR


: "${SA_PASSWORD:?Missing SA_PASSWORD}" 
: "${DB:?Missing DB}"
: "${USER:?Missing USER}"


# Get list of tables
TABLES=$(docker exec -i $CONTAINER /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U $USER -P $SA_PASSWORD  -C -d $DB -h -1 -W \
  -Q "SET NOCOUNT ON; 
     SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")

echo "Found tables:"

mkdir -p "$EXPORT_DIR"
# Loop and export each table
for T in $TABLES; do
  CLEAN=$(echo $T | tr -d '\r')  # remove CRLF
  OUTFILE="${CLEAN//./_}.csv"

  echo "Exporting $CLEAN → $EXPORT_DIR/$OUTFILE"
docker exec -i "$CONTAINER" /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U "$USER" -P "$SA_PASSWORD" -C -d "$DB" \
  -h 1 -W \
  -Q "SET NOCOUNT ON; SELECT TOP 0 * FROM $CLEAN" \
  -s"," | grep -v '^-' > "$EXPORT_DIR/$OUTFILE"


#h 0 top header -W removes trailing spaces, -s"," sets comma as separator
  docker exec -i $CONTAINER /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U $USER -P $SA_PASSWORD -C -h -1 -W -d $DB \
    -Q "SET NOCOUNT ON; SELECT * FROM $CLEAN" \
    -s"," -W >> "$EXPORT_DIR/$OUTFILE"
done

echo "Done."
