
package com.demo.kafka.spark.persist;

import java.io.Serializable;

public class Order implements Serializable {

    /**
     * @Fields serialVersionUID : TODO
     */

    private static final long serialVersionUID = 1L;

    private String            name;
    private Long              price;
    private String            data;

    public Order(){

    }

    public Order(String name, Long price){
        this.name = name;
        this.price = price;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

}
