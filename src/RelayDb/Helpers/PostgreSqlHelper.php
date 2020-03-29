<?php
namespace RelayDb\Helpers;

use RelayDb\ChunkFetcher;
use RelayDb\Exceptions\RelayDbException;
use PDO;
use PDOStatement;
use RecursiveIteratorIterator;
use RecursiveArrayIterator;

class PostgreSqlHelper
{
    public $pdoHelper;

    /**
     * SqliteHelper constructor.
     * @param PdoHelper|null $pdoHelper
     */
    public function __construct(PdoHelper $pdoHelper = null)
    {
        $this->pdoHelper = $pdoHelper ?? new PdoHelper();
    }

    /**
     * @param PDOStatement $stmt
     * @return array
     * @throws RelayDbException
     */
    public function makeColumnDeclarationsFromStatement(PDOStatement $stmt): array
    {
        return $this->makeColumnDeclarationsFromColumnMetas($this->pdoHelper->getColumnMetas($stmt));
    }

    /**
     * @param array $columnMetas
     * @return array
     * @throws RelayDbException
     */
    public function makeColumnDeclarationsFromColumnMetas(array $columnMetas): array
    {
        $declarations = [];
        foreach ($columnMetas as $meta) {
            $declarations[] = $this->makeColumnDeclaration($meta);
        }
        return $declarations;
    }

    /**
     * @param array $columnMeta
     * @return string
     * @throws RelayDbException
     */
    public function makeColumnDeclaration(array $columnMeta)
    {
        $name = (string)($columnMeta['name'] ?? '');
        if ($name === '') {
            throw new RelayDbException('no name:' . json_encode($columnMeta));
        }

        $nativeType = (string)($columnMeta['native_type'] ?? '');
        if (empty($nativeType)) {
            throw new RelayDbException('no type:' . json_encode($columnMeta));
        }

        $precision = (int)($columnMeta['precision'] ?? 0) - 4;

        switch ($nativeType) {
            case 'varchar':
                if ($precision <= 0) {
                    $type = 'TEXT';
                } else {
                    $type = "VARCHAR({$precision})";
                }
                break;
            default:
                $type = strtoupper($nativeType);
                break;
        }

        return $name . ' ' . $type;
    }

    /**
     * @param array $columns
     * @return string
     */
    public function makePrimaryDeclarations(array $columns): string
    {
        $primary = implode(' ,', $columns);
        return "PRIMARY KEY ({$primary})";
    }

    /**
     * @param PDO $pdo
     * @param string $table
     * @param array $columns
     * @param string $primary
     */
    public function createTable(PDO $pdo, string $table, array $columns, string $primary)
    {
        $declarations = implode(', ', array_merge($columns, [$primary]));

        $sql = "CREATE TABLE IF NOT EXISTS {$table} ({$declarations});";

        echo var_export($sql, true) . PHP_EOL;
        $pdo->exec($sql);
    }

    /**
     * @param PDO $pdo
     * @param string $table
     * @param array $columns
     * @param array $primaryColumns
     * @param array $rows
     */
    public function bulkReplace(PDO $pdo, string $table, array $columns, array $primaryColumns, array $rows)
    {
        $updateColumns = $this->pdoHelper->makeColumnsForUpdate($columns, $primaryColumns);
        $updateIndices = $this->pdoHelper->makeUpdateIndices($columns, $updateColumns);
        $sql = $this->pdoHelper->makeReplacementSqlForUpsert($table, $columns, $primaryColumns, $updateColumns);

        $stmt = $pdo->prepare($sql);
        foreach ($rows as $row) {
            $values = [];
            foreach ($row as $v) {
                if (is_bool($v)) {
                    $values[] = $v ? '1' : '0';
                } else {
                    $values[] = $v;
                }
            }

            var_dump(array_merge($values, $this->pdoHelper->makeUpdateValues($updateIndices, $values)));
            $stmt->execute(array_merge($values, $this->pdoHelper->makeUpdateValues($updateIndices, $values)));
        }
    }
}
