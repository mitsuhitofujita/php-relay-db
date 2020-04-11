<?php
namespace RelayDb;

use RelayDb\Helpers\PdoHelper;
use RelayDb\Exceptions\RelayDbException;
use RelayDb\Helpers\PostgreSqlHelper;
use PDO;
use PDOStatement;
use Exception;

class RelayPg
{
    /**
     * @var PdoHelper
     */
    public $pdoHelper;

    /**
     * @var PostgreSqlHelper|null
     */
    public $pgHelper;

    /**
     * FromDb constructor.
     * @param PdoHelper|null $pdoHelper
     * @param PostgreSqlHelper|null $pgHelper
     */
    public function __construct(PdoHelper $pdoHelper = null, PostgreSqlHelper $pgHelper = null)
    {
        $this->pdoHelper = $pdoHelper ?? new PdoHelper();
        $this->pgHelper = $pgHelper ?? new PostgreSqlHelper();
    }

    /**
     * @param array $settings
     * @throws Exception
     */
    public function relay(array $settings): void
    {
        $source = $settings['source'];
        $srcPdo = $this->pdoHelper->makePdo(
            $source['dsn'],
            $source['username'],
            $source['password'],
            $source['options']
        );

        $relay = $settings['relay'];
        $pdo = $this->pdoHelper->makePdo($relay['dsn'], $relay['username'], $relay['password'], $relay['options']);

        $this->pdoHelper->transaction($pdo, function ($pdo) use ($srcPdo, $settings) {
            $this->relayTables($pdo, $srcPdo, $settings);
            $this->relayRelations($pdo, $srcPdo, $settings);
        });
    }

    /**
     * @param PDO $pdo
     * @param PDO $srcPdo
     * @param array $settings
     */
    public function relayTables(PDO $pdo, PDO $srcPdo, array $settings): void
    {
        $defaultTableSetting = [
            'source_limit' => 1000,
        ];
        foreach ($settings['tables'] as $tableSetting) {
            $tableSetting += $defaultTableSetting;
            $this->relayTable(
                $pdo,
                $tableSetting['table'],
                $tableSetting['primary_columns'],
                $srcPdo,
                $tableSetting['source_query'],
                $tableSetting['source_limit']
            );
        }
    }

    /**
     * @param PDO $pdo
     * @param string $table
     * @param array $primaryColumns
     * @param PDO $srcPdo
     * @param string $srcQuery
     * @param int $limit
     */
    public function relayTable(
        PDO $pdo,
        string $table,
        array $primaryColumns,
        PDO $srcPdo,
        string $srcQuery,
        int $limit
    ): void {
        (new ChunkFetcher($srcPdo, $limit))->bulkRows(
            $srcQuery,
            [],
            function ($srcStmt, $i) use ($pdo, $table, $primaryColumns) {
                if ($i === 0) {
                    $this->createRelayTable($pdo, $table, $primaryColumns, $srcStmt);
                }
                return $this->replaceRelayTable($pdo, $table, $primaryColumns, $srcStmt);
            }
        );
    }

    /**
     * @param PDO $pdo
     * @param string $table
     * @param array $primaryColumns
     * @param PDOStatement $srcStmt
     * @throws RelayDbException
     */
    public function createRelayTable(PDO $pdo, string $table, array $primaryColumns, PDOStatement $srcStmt): void
    {
        $columnMetas = $this->pdoHelper->getColumnMetas($srcStmt);
        $columns = $this->pgHelper->makeColumnDeclarationsFromColumnMetas($columnMetas);
        $primary = $this->pgHelper->makePrimaryDeclarations($primaryColumns);
        $this->pgHelper->createTable($pdo, $table, $columns, $primary);
    }

    /**
     * @param PDO $pdo
     * @param string $table
     * @param PDOStatement $stmt
     * @return bool
     */
    public function replaceRelayTable(PDO $pdo, string $table, array $primaryColumns, PDOStatement $stmt): bool
    {
        $columns = $this->pdoHelper->getColumns($stmt);
        if (empty($columns)) {
            return false;
        }

        $rows = $stmt->fetchAll();
        if (empty($rows)) {
            return false;
        }

        $this->pgHelper->bulkReplace($pdo, $table, $columns, $primaryColumns, $rows);
        return true;
    }

    /**
     * @param PDO $pdo
     * @param PDO $srcPdo
     * @param array $settings
     */
    public function relayRelations(PDO $pdo, PDO $srcPdo, array $settings): void
    {
        $defaultRelationSettings = [
            'relay_limit' => 1000,
            'source_limit' => 1000,
        ];
        foreach ($settings['relations'] as $relationSetting) {
            $this->relayRelation($pdo, $srcPdo, $relationSetting + $defaultRelationSettings);
        }
    }

    /**
     * @param PDO $pdo
     * @param PDO $srcPdo
     * @param array $relationSetting
     */
    public function relayRelation(PDO $pdo, PDO $srcPdo, array $relationSetting): void
    {
        (new ChunkFetcher($pdo, $relationSetting['relay_limit']))->bulkRows(
            $relationSetting['relay_query'],
            [],
            function ($relStmt, $i) use ($pdo, $srcPdo, $relationSetting) {
                $resultPartialQuery = $this->makeResultPartialQuery($relStmt);
                if ($resultPartialQuery === '') {
                    return false;
                }

                $srcQuery = str_replace(
                    '{{relay_query_result}}',
                    $resultPartialQuery,
                    $relationSetting['source_query']
                );
                $this->relayTable(
                    $pdo,
                    $relationSetting['table'],
                    $relationSetting['primary_columns'],
                    $srcPdo,
                    $srcQuery,
                    $relationSetting['source_limit']
                );

                return true;
            }
        );
    }

    /**
     * @param PDOStatement $stmt
     * @return string
     */
    public function makeResultPartialQuery(PDOStatement $stmt): string
    {
        $partialQuery = [];
        foreach ($stmt as $row) {
            $partialQuery[] = '(' . implode(' ,', $row) . ')';
        }
        return implode(' ,', $partialQuery);
    }
}
