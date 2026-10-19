package ru.pw8.supplier;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/suppliers")
public class SupplierController {

    private final JdbcTemplate jdbc;

    public SupplierController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record Product(
            long id, long supplierId, String name, BigDecimal price, int availableQuantity) {}

    @GetMapping
    public Mono<List<Map<String, Object>>> suppliers() {
        return Mono.fromCallable(() -> jdbc.queryForList("SELECT * FROM suppliers ORDER BY id"))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/products")
    public Mono<List<Product>> products() {
        return Mono.fromCallable(
                        () -> jdbc.query("SELECT * FROM products ORDER BY id", this::mapProduct))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/products/{id}")
    public Mono<Product> product(@PathVariable long id) {
        return products().map(products -> findProduct(products, id));
    }

    private Product findProduct(List<Product> products, long id) {
        return products.stream()
                .filter(product -> product.id() == id)
                .findFirst()
                .orElseThrow(
                        () ->
                                new ResponseStatusException(
                                        HttpStatus.NOT_FOUND, "Product not found"));
    }

    private Product mapProduct(ResultSet result, int rowNumber) throws SQLException {
        return new Product(
                result.getLong("id"),
                result.getLong("supplier_id"),
                result.getString("name"),
                result.getBigDecimal("price"),
                result.getInt("available_quantity"));
    }
}
