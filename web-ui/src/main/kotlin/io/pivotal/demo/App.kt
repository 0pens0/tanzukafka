package io.pivotal.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.web.bind.annotation.GetMapping

@SpringBootApplication
@Controller
class App {

    @GetMapping("/")
    fun index(model: Model): String {
        model.addAttribute("username", "tanzukafka")
        return "index"
    }
}

fun main(args: Array<String>) {
	runApplication<App>(*args)
}
