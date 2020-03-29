<?php
namespace RelayDb;

use PDO;

class ChunkFetcher
{
    private PDO $pdo;
    private int $limit;

    /**
     * ChunkFetcher constructor.
     * @param PDO $pdo
     * @param int $limit
     */
    public function __construct(PDO $pdo, $limit = 10000)
    {
        $this->pdo = $pdo;
        $this->limit = $limit;
    }

    /**
     * @param string $sql
     * @param array $binds
     * @param callable $callback
     * @return void
     */
    public function bulkRows(string $sql, array $binds, callable $callback): void
    {
        $limit = $this->limit;
        $stmt = $this->pdo->prepare(rtrim($sql, ';') . ' LIMIT ? OFFSET ?');
        for ($i = 0; $stmt->execute(array_merge($binds, [$limit, $i * $limit])) !== false; $i++) {
            echo var_export($sql, true) . PHP_EOL;
            echo var_export($binds, true) . PHP_EOL;
            if ($callback($stmt, $i) === false) {
                break;
            }
        }
    }

    /**
     * @param string $sql
     * @param array $binds
     * @param callable $callback
     * @return void
     */
    public function eachRows(string $sql, array $binds, callable $callback): void
    {
        $this->bulkRows($sql, $binds, function ($stmt, $i) use ($callback) {
            foreach ($stmt as $j => $row) {
                $callback($row, $i, $j);
            }
        });
    }
}
