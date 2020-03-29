<?php

require __DIR__ . '/../vendor/autoload.php';

use RelayDb\StorePg;

class Store
{
    public function main(): void
    {
        $settings = [
            'destination' => [
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
                    'table' => 'users',
                    'primary_columns' => ['id'],
                    'query' => 'SELECT * FROM users ORDER BY id',
                ],
                [
                    'table' => 'accounts',
                    'primary_columns' => ['id'],
                    'query' => 'SELECT * FROM accounts ORDER BY id',
                ],
            ],
        ];

        $toDb = new StorePg();
        $toDb->store($settings);
    }
}

try {
    $sample = new Store();
    $sample->main();
} catch (Exception $e) {
    echo $e->getMessage();
}



