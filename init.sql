CREATE TABLE dim_lojas (
  id char(1),
  logradouro varchar(1000),
  cidade varchar(100),
  estado char(2)
);
CREATE TABLE dim_produtos (id char(3), produto varchar(1000), valor float);
CREATE TABLE fato_pedidos (
  id char(3),
  produto char(3),
  loja char(1),
  data date
);
INSERT INTO dim_lojas
VALUES (
    '1',
    'Avenida Washington Soares 3636',
    'Fortaleza',
    'CE'
  ),
  ('2', 'Avenida Paulista 1106', 'São Paulo', 'SP'),
  (
    '3',
    'Avenida Nossa Senhora de Copacabana 777',
    'Rio de Janeiro',
    'RJ'
  ),
  (
    '4',
    'Avenida Sete de Setembro 202',
    'Salvador',
    'BA'
  );
INSERT INTO dim_produtos
VALUES ('101', 'Notebook Dell Inspiron 15 3000', 3299.99),
  ('102', 'Notebook HP 246 G7', 3771.08),
  ('103', 'Notebook Samsung Book X40', 3514.05),
  ('104', 'Notebook Lenovo Core i5', 3419.05),
  ('105', 'Smartphone Xiaomi Redmi Note 9', 1524.98),
  ('106', 'Smartphone Samsung Galaxy A30s', 1499),
  (
    '107',
    'Smartphone Samsung Galaxy Note 10 Lite',
    1999
  ),
  ('108', 'Smartphone LG K61', 1347.57),
  ('109', 'Smartphone Moto G8', 1254.57),
  ('110', 'Smartphone iPhone 11', 4399),
  ('111', 'Tablet Samsung Galaxy Tab A', 1299),
  ('112', 'Tablet Multilaser M10A', 899),
  ('113', 'Tablet iPad 7', 3377),
  ('114', 'Fone de ouvido JBL Tune 500', 239),
  ('115', 'Fone de ouvido AirPods', 899),
  ('116', 'Fone de ouvido Xiaomi AirDots', 168.9),
  ('117', 'Mouse Dell WM126', 104.99),
  ('118', 'Mouse Logitech M280', 89.99),
  ('119', 'Console PlayStation 5', 4999),
  ('120', 'Console Xbox Series X', 4369.9);
INSERT INTO fato_pedidos
VALUES ('101', '101', '1', '20200605'),
  ('101', '117', '1', '20200605'),
  ('102', '103', '1', '20200605'),
  ('103', '110', '2', '20200605'),
  ('103', '115', '2', '20200605'),
  ('104', '111', '2', '20200605'),
  ('105', '109', '2', '20200605'),
  ('105', '117', '2', '20200605'),
  ('106', '102', '2', '20200605'),
  ('107', '119', '2', '20200605'),
  ('108', '120', '3', '20200605'),
  ('109', '101', '3', '20200605'),
  ('109', '118', '3', '20200605'),
  ('110', '105', '3', '20200605'),
  ('110', '116', '3', '20200605'),
  ('111', '103', '3', '20200605'),
  ('111', '107', '3', '20200605'),
  ('111', '111', '3', '20200605'),
  ('111', '117', '3', '20200605'),
  ('112', '104', '1', '20200606'),
  ('113', '110', '1', '20200606'),
  ('114', '112', '1', '20200606'),
  ('115', '108', '1', '20200606'),
  ('115', '114', '1', '20200606'),
  ('116', '103', '1', '20200606'),
  ('117', '118', '1', '20200606'),
  ('118', '120', '2', '20200606'),
  ('119', '107', '2', '20200606'),
  ('119', '114', '2', '20200606'),
  ('120', '102', '2', '20200606'),
  ('120', '112', '2', '20200606'),
  ('120', '116', '2', '20200606'),
  ('120', '118', '2', '20200606'),
  ('121', '119', '2', '20200606'),
  ('122', '102', '2', '20200606'),
  ('122', '117', '2', '20200606'),
  ('123', '119', '2', '20200606'),
  ('124', '119', '3', '20200606'),
  ('125', '106', '3', '20200606'),
  ('125', '111', '3', '20200606'),
  ('126', '110', '3', '20200606'),
  ('127', '110', '3', '20200606'),
  ('127', '115', '3', '20200606'),
  ('128', '120', '3', '20200606'),
  ('129', '114', '3', '20200606'),
  ('129', '115', '3', '20200606'),
  ('129', '116', '3', '20200606'),
  ('130', '105', '3', '20200606'),
  ('130', '116', '3', '20200606'),
  ('130', '119', '3', '20200606');
create database destiny;