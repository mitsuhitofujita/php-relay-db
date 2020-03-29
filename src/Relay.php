<?php

require __DIR__ . '/../vendor/autoload.php';

use RelayDb\RelayPg;

class Relay
{
    public function main(): void
    {
        $settings = [
            'source' => [
                'dsn' => 'pgsql:dbname=dev;host=postgresql',
                'username' => 'dev',
                'password' => 'dev',
                'options' => [],
            ],
            'relay' => [
                'dsn' => 'pgsql:dbname=relay;host=postgresql',
                'username' => 'dev',
                'password' => 'dev',
                'options' => [],
            ],
            'tables' => [
                [
                    'source_query' => 'SELECT * FROM users WHERE id IN (1, 2) ORDER BY id',
                    'table' => 'users',
                    'primary_columns' => ['id'],
                ],
            ],
            'relations' => [
                [
                    'relay_query' => 'SELECT id FROM users ORDER BY id',
                    'source_query' => 'SELECT * FROM accounts WHERE user_id IN ({{relay_query_result}}) ORDER BY id',
                    'table' => 'accounts',
                    'primary_columns' => ['id'],
                    'relay_limit' => 100,
                ],
            ],
        ];

        $relayPg = new RelayPg();
        $relayPg->relay($settings);
    }
}

try {
    $sample = new Relay();
    $sample->main();
} catch (Exception $e) {
    echo $e->getMessage();
}



