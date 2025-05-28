<?php

$finder = (new PhpCsFixer\Finder())
    ->in([__DIR__ . '/Classes', __DIR__ . '/Tests']);

return (new PhpCsFixer\Config())
    ->setParallelConfig(PhpCsFixer\Runner\Parallel\ParallelConfigFactory::detect())
    ->setRules([
        '@PSR12' => true,
        '@PHP82Migration' => true,
        'yoda_style' => false,
        'concat_space' => ['spacing' => 'one'],
        'no_unused_imports' => false,
        'no_superfluous_phpdoc_tags' => false,
        'phpdoc_types_order' => ['null_adjustment' => 'always_last'],
        'class_definition' => ['multi_line_extends_each_single_line' => true],
        'global_namespace_import' => ['import_classes' => true],
        'phpdoc_annotation_without_dot' => false,
        'phpdoc_separation' => false,
        'cast_spaces' => ['space' => 'none'],
        'phpdoc_summary' => false,
    ])
    ->setFinder($finder);
