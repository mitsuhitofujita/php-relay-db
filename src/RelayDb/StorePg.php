<?php
namespace RelayDb;

use RelayDb\Helpers\PdoHelper;
use RelayDb\Helpers\PostgreSqlHelper;
use PDO;

class StorePg
{
    /**
     * @var PdoHelper
     */
    public $pdoHelper;

    /**
     * @var PostgreSqlHelper
     */
    public $pgHelper;

    /**
     * StoreDb constructor.
     * @param PdoHelper|null $pdoHelper
     * @param PostgreSqlHelper|null $sqliteHelper
     */
    public function __construct(PdoHelper $pdoHelper = null, PostgreSqlHelper $pgHelper = null)
    {
        $this->pdoHelper = $pdoHelper ?? new PdoHelper();
        $this->pgHelper = $pgHelper ?? new PostgreSqlHelper();
    }

    /**
     * @param array $settings
     * @throws \Exception
     */
    public function store(array $settings): void
    {
        $dst = $settings['destination'];
        $dstPdo = $this->pdoHelper->makePdo($dst['dsn'], $dst['username'], $dst['password'], $dst['options']);

        $relay = $settings['relay'];
        $pdo = $this->pdoHelper->makePdo($relay['dsn'], $relay['username'], $relay['password'], $relay['options']);

        $this->pdoHelper->transaction($pdo, function($pdo) use ($dstPdo, $settings) {
            $this->storeTables($pdo, $dstPdo, $settings);
        });
    }

    public function storeTables(PDO $pdo, PDO $dstPdo, array $settings) {
        $defaultTableSetting = [
            'relay_limit' => 1000,
        ];
        foreach ($settings['tables'] as $tableSetting) {
            $tableSetting += $defaultTableSetting;
            $this->storeTable(
                $pdo,
                $tableSetting['table'],
                $tableSetting['primary_columns'],
                $tableSetting['query'],
                $tableSetting['relay_limit'],
                $dstPdo
            );
        }
    }

    public function storeTable(PDO $pdo, string $table, array $primaryColumns, string $query, int $limit, PDO $dstPdo): void
    {
        (new ChunkFetcher($pdo, $limit))->bulkRows(
            $query,
            [],
            function ($stmt, $i) use ($dstPdo, $table, $primaryColumns) {
                $columns = $this->pdoHelper->getColumns($stmt);
                if (empty($columns)) {
                    return false;
                }

                $rows = $stmt->fetchAll();
                if (empty($rows)) {
                    return false;
                }

                $this->pgHelper->bulkReplace($dstPdo, $table, $columns, $primaryColumns, $rows);
                return true;
            }
        );
    }
}
