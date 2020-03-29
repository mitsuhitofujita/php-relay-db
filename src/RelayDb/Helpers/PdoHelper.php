<?php
namespace RelayDb\Helpers;

use PDO;
use PDOStatement;
use Exception;

class PdoHelper
{
    /**
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     * @return PDO
     */
    public function makePdo(string $dsn, string $username, string $password, array $options = []): PDO
    {
        return new PDO(
            $dsn,
            $username,
            $password,
            [
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_NUM,
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => false,
            ] + $options
        );
    }

    /**
     * @param PDO $pdo
     * @param callable $f
     * @throws Exception
     */
    public function transaction(PDO $pdo, callable $f): void
    {
        $pdo->beginTransaction();
        try {
            $f($pdo);
            $pdo->commit();
        } catch (Exception $exception) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $exception;
        }
    }

    /**
     * @param PDOStatement $stmt
     * @return array
     */
    public function getColumns(PDOStatement $stmt): array
    {
        $columns = [];
        for ($i = 0; $i < $stmt->columnCount(); $i++) {
            $columnMeta = $stmt->getColumnMeta($i);
            if ($columnMeta === false) {
                return [];
            }
            $columns[] = $columnMeta['name'];
        }
        return $columns;
    }

    /**
     * @param array $columnMetas
     * @return array
     */
    public function getColumnsFromColumnMetas(array $columnMetas): array
    {
        return array_column($columnMetas, 'name');
    }

    /**
     * @param PDOStatement $stmt
     * @return array
     */
    public function getColumnMetas(PDOStatement $stmt): array
    {
        $columnMetas = [];
        for ($i = 0; $i < $stmt->columnCount(); $i++) {
            $columnMeta = $stmt->getColumnMeta($i);
            if ($columnMeta === false) {
                return [];
            }
            $columnMetas[] = $columnMeta;
        }
        return $columnMetas;
    }

    /**
     * @param array $binds
     * @return string
     */
    public function makeReplacementSqlForPrepare(array $binds): string
    {
        return '(' . implode(',', array_pad([], count($binds), '?')) . ')';
    }

    /**
     * @param array $columns
     * @return string
     */
    public function makeFieldsSqlForInsert(array $columns): string
    {
        return '(' . implode(',', $columns) . ')';
    }

    /**
     * @param array $columns
     * @return string
     */
    public function makeReplacementSqlForInsert(array $columns): string
    {
        return '(' . implode(',', array_pad([], count($columns), '?')) . ')';
    }

    /**
     * @param array $rows
     * @return string
     */
    public function makeReplacementSqlForBulkInsert(array $rows): string
    {
        $valuesSql = $this->makeReplacementSqlForInsert(array_values($rows[0] ?? []));
        return implode(',', array_pad([], count($rows), $valuesSql));
    }

    /**
     * @param array $columns
     * @return string
     */
    public function makeReplacementSqlForUpdate(array $columns): string
    {
        return implode(',', array_map(function ($column) {
            return "{$column}=?";
        }, $columns));
    }

    public function makePrimariesSql(array $primaryColumns): string
    {
        return '(' . implode(' ,', $primaryColumns) . ')';
    }

    /**
     * @param array $columns
     * @param array $primaryColumns
     * @return array
     */
    public function makeColumnsForUpdate(array $columns, array $primaryColumns)
    {
        return array_filter($columns, function ($column) use ($primaryColumns) {
            return !in_array($column, $primaryColumns);
        });
    }

    /**
     * @param $table
     * @param $insert_columns
     * @param $update_columns
     * @return string
     */
    public function makeReplacementSqlForUpsert($table, $insertColumns, $primaryColumns, $updateColumns)
    {
        return <<<SQL
INSERT INTO {$table}
    {$this->makeFieldsSqlForInsert($insertColumns)}
VALUES
    {$this->makeReplacementSqlForInsert($insertColumns)}
ON CONFLICT {$this->makePrimariesSql($primaryColumns)}
DO UPDATE SET 
    {$this->makeReplacementSqlForUpdate($updateColumns)}
SQL;
    }

    /**
     * @param $columns
     * @param $primaries
     * @return array
     */
    public function makeUpdateIndices(array $columns, array $updates): array
    {
        $indices = [];
        foreach ($columns as $i => $column) {
            if (in_array($column, $updates)) {
                $indices[] = $i;
            }
        }
        return $indices;
    }

    /**
     * @param array $indices
     * @param array $row
     * @return array
     */
    public function makeUpdateValues(array $indices, array $row): array
    {
        $values = [];
        foreach ($indices as $index) {
            $values[] = $row[$index];
        }
        return $values;
    }
}
