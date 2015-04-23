CREATE TABLE effort (
    id INT NOT NULL AUTO_INCREMENT,
    fin TIMESTAMP, PRIMARY KEY (id)
);

CREATE TABLE triad_type (
    id SMALLINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(4) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE triad (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    a_id INT UNSIGNED NOT NULL,
    b_id INT UNSIGNED NOT NULL,
    c_id INT UNSIGNED NOT NULL,
    triad_type_id SMALLINT NOT NULL,
    first_seen TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY specific_triad (a_id, b_id, c_id, triad_type_id),
    FOREIGN KEY (triad_type_id) REFERENCES triad_type (id)
);

CREATE TABLE triad_change (
    from_triad INT UNSIGNED NOT NULL,
    to_triad INT UNSIGNED NOT NULL,
    PRIMARY KEY (from_triad, to_triad),
    FOREIGN KEY (from_triad) REFERENCES triad (id),
    FOREIGN KEY (to_triad) REFERENCES triad (id)
)
