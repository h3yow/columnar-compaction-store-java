package com.github.h3yow.columnarcompactionstorejava.web;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionResult;

/** 压实单独一个端点。 */
@RestController
@RequestMapping("/api/store")
public class CompactionController {

    private final StoreService service;

    public CompactionController(StoreService service) {
        this.service = service;
    }

    @PostMapping("/compact")
    public CompactionResult compact() {
        return service.get().compact();
    }
}
